package com.oracle.jsonloader.command.mongodbbsontoajdosonloading;

import com.oracle.jsonloader.exception.*;
import oracle.jdbc.OracleConnection;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.oracle.jsonloader.util.Console.print;
import static com.oracle.jsonloader.util.Console.println;

public class MongoDBBSONToAJDOSONLoading {
    public static void main(final String[] args) throws JSONLoaderException {
        new MongoDBBSONToAJDOSONLoading(args).run();
    }

    public static int BATCH_SIZE = 5000;
    public static boolean KEEP_MONGODB_OBJECTIDS = true;

    private String dumpDir;
    private String databaseName;
    private String ajdConnectionService;
    private String user;
    private String password;
    private String oracleMetadataFilename = null;
    private final Properties oracleMetadata = new Properties();

    public MongoDBBSONToAJDOSONLoading(String[] args) throws JSONLoaderException {
        analyzeCLIParameters(args);
    }

    /**
     * Detect all collections from the given mongodb dump file and database directory as BSON format and upload the JSON documents converted to OSON into an AJD database.
     *
     * @throws JSONLoaderException
     */
    private void run() throws JSONLoaderException {
        final File dataDir = new File(new File(dumpDir), databaseName);

        if (!dataDir.exists() && dataDir.isDirectory())
            throw new SourceDirectoryNotFoundException(dataDir.getAbsolutePath());

        if(oracleMetadataFilename != null) {
            final File oracleMetadataFile = new File(oracleMetadataFilename);
            if(!oracleMetadataFile.exists()) {
                System.out.println("Oracle metadata file "+oracleMetadataFile+" doesn't exist!");
                System.exit(-1);
            }

            try {
                oracleMetadata.load(new FileInputStream(oracleMetadataFile));
            } catch (IOException e) {
                System.err.println("Can't load file "+oracleMetadataFile+"!");
                e.printStackTrace();
                System.exit(-2);
            }
        }

        // Find all collection related metadata files being json or gzipped json
        final File[] metadataFiles = dataDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".metadata.json") || name.toLowerCase().endsWith(".metadata.json.gz"));

        final List<JSONCollection> collections = new ArrayList<>();

        println("================================================================================");
        int cores = Runtime.getRuntime().availableProcessors();
        if (cores > 1) cores--;

        PoolDataSource pds = null;
        try {
            print("Initializing connection pool ...");
            pds = initializeConnectionPool(ajdConnectionService, user, password, cores);

            //test(pds);

            println(" OK");

            println("Collections:");

            // Listing all collections
            for (File f : metadataFiles) {
                final JSONCollection collection = new JSONCollection(f.getName().substring(0, f.getName().length() - (f.getName().endsWith(".gz") ? 17 : 14)), f, pds);
                println("- Found collection " + collection.name);
                collections.add(collection);
            }

            // Loading all collections
            int collectionNumber = 1;
            for (JSONCollection c : collections) {
                println(String.format("  . Loading collection [%d/%d]: %s...", collectionNumber, collections.size(), c.name));
                c.findDatafiles();
                c.load(cores,oracleMetadata);
                collectionNumber++;
            }
        } catch (Throwable t) {
            throw new UnknownException(ErrorCode.Unknown, "Unhandled error", t);
        }
    }

    private void test(PoolDataSource pds) {
        OracleConnection conn = null;

        try {
            conn = (OracleConnection) pds.getConnection();
            OracleRDBMSClient cl = new OracleRDBMSClient();

            // Get a database.
            OracleDatabase db = cl.getDatabase(conn);

            OracleCollection col = db.admin().createCollection("myCol");

            // Point at Long Beach
            String docStr1 = "{\"location\" : {\"type\": \"Point\", \"coordinates\": [33.7243,-118.1579]} }";

            // LineString near Las Vegas
            String docStr2 = "{\"location\" : {\"type\" : \"LineString\", \"coordinates\" : " +
                    "[[36.1290,-115.1037], [35.0869,-114.9499], [36.0846,-115.3234]]} }";

            // Polygon near Phoenix
            String docStr3 = "{\"location\" : {\"type\" : \"Polygon\", \"coordinates\" : " +
                    "[[[33.4222,-112.0605], [33.3855,-112.2253], [33.3121,-112.1044], [33.3305,-111.8737], " +
                    "[33.4222,-112.0605]]]} }";

            String key1, key2, key3;
            OracleDocument filterDoc = null, doc = null;

            doc = col.insertAndGet(db.createDocumentFromString(docStr1));
            key1 = doc.getKey();
            doc = col.insertAndGet(db.createDocumentFromString(docStr2));
            key2 = doc.getKey();
            doc = col.insertAndGet(db.createDocumentFromString(docStr3));
            key3 = doc.getKey();

            // create spatial index with mixed case index name
            String indexSpec = null, indexName = "locationIndex";
            indexSpec = "{\"name\" : \"" + indexName + "\", \"spatial\" : \"location\"}";
            col.admin().createIndex(db.createDocumentFromString(indexSpec));

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        System.exit(-1000);
    }

    private PoolDataSource initializeConnectionPool(String ajdConnectionService, String user, String password, int cores) throws SQLException, IOException {
        PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
        pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");

        //pds.setURL("jdbc:oracle:thin:@//localhost/PDB1");
        //System.out.println("jdbc:oracle:thin:@" + ajdConnectionService + "?TNS_ADMIN=" + new File("wallet").getCanonicalPath().replace('\\', '/'));
        pds.setURL("jdbc:oracle:thin:@" + ajdConnectionService + "?TNS_ADMIN=" + new File("wallet").getCanonicalPath().replace('\\', '/'));
        pds.setUser(user);
        pds.setPassword(password);
        pds.setConnectionPoolName("JDBC_UCP_POOL:" + Thread.currentThread().getName());
        pds.setInitialPoolSize(cores);
        pds.setMinPoolSize(cores);
        pds.setMaxPoolSize(cores);
        pds.setTimeoutCheckInterval(30);
        pds.setInactiveConnectionTimeout(120);
        pds.setValidateConnectionOnBorrow(true);
        pds.setMaxStatements(20);
        pds.setConnectionProperty(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");

        return pds;
    }

    private void analyzeCLIParameters(String[] args) throws JSONLoaderException {
        if (args.length < 6) throw new BadCLIUsageException();

        for (int i = 1; i < args.length; i++) {
            switch (i) {
                case 1:
                    dumpDir = args[i];
                    break;

                case 2:
                    databaseName = args[i];
                    break;

                case 3:
                    ajdConnectionService = args[i];
                    break;

                case 4:
                    user = args[i];
                    break;

                case 5:
                    password = args[i];
                    break;

                case 6:
                    BATCH_SIZE = Integer.parseInt(args[i]);
                    break;

                case 7:
                    KEEP_MONGODB_OBJECTIDS = Boolean.parseBoolean(args[i]);
                    break;

                case 8:
                    oracleMetadataFilename = args[i];
                    break;

            }
        }

        println("> Source dump directory         : " + dumpDir);
        println("> Source database name          : " + databaseName);
        println("> Destination database service  : " + ajdConnectionService);
        println("> Destination database user name: " + user);
        println("> JSON loading batch size       : " + BATCH_SIZE);
        println("> Keep MongoDB ObjectIDs        : " + KEEP_MONGODB_OBJECTIDS);
        if(oracleMetadataFilename != null) {
            println("> Oracle JSON Metadata file     : " + oracleMetadataFilename);
        }
    }
}
