package com.oracle.jsonloader.command.mongodbbsontoajdosonloading;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.oracle.jsonloader.exception.SODACollectionCantKeepMongoDBIdException;
import com.oracle.jsonloader.exception.SODACollectionIdNotAutomaticallyGeneratedException;
import com.oracle.jsonloader.exception.SODACollectionMetadataUnmarshallException;
import com.oracle.jsonloader.model.*;
import com.oracle.jsonloader.util.BSONCollectionFilenameFilter;
import com.oracle.jsonloader.util.BlockingQueueCallback;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static com.oracle.jsonloader.util.Console.print;
import static com.oracle.jsonloader.util.Console.println;

public class JSONCollection implements BlockingQueueCallback {
    public final String name;
    public final File metadata;
    public final List<File> dataFiles = new ArrayList<>();
    public MongoDBMetadata mongoDBMetadata;
    private PoolDataSource pds;

    public JSONCollection(final String name, final File metadata, final PoolDataSource pds) {
        this.name = name;
        this.metadata = metadata;
        this.pds = pds;
    }

    /**
     * Discover data files associated with this collection using either .bson or .bson.gz file extension.
     */
    public void findDatafiles() {
        long totalSize = 0;

        for (File f : metadata.getParentFile().listFiles(new BSONCollectionFilenameFilter(name))) {
            if (f.isFile() && f.length() > 0) {// && f.getName().startsWith(name) && (f.getName().endsWith(".bson") || f.getName().endsWith(".bson.gz"))) {
                dataFiles.add(f);
                totalSize += f.length();
            }
        }

        println(String.format("\t- found %d data file(s) (%.3f MB)", dataFiles.size(), (double) totalSize / 1024.0 / 1024.0));
    }

    /**
     * Load the collection using all the data files found!
     *
     * @throws Exception
     */
    public void load(int cores, Properties oracleMetadata) throws Exception {
        // ensure collection exists!
        createSODACollectionIfNotExists(name);

        // Load metadata file
        //println("\t- metadata file: " + metadata.getName());
        loadMongoDBMetadataContent();

        boolean mustHaveASearchIndex = false;
        for (MetadataIndex index : mongoDBMetadata.getIndexes()) {
            if (index.getName().contains("$**")) {
                mustHaveASearchIndex = true;
                //println("\t\t. " + index.getName() + ": (a JSON search index will be created later)");
            } else if (index.getKey().text) {
                mustHaveASearchIndex = true;
                //println("\t\t. " + index.getName() + ": (a JSON search index will be created later)");
            }
        }

        if (mustHaveASearchIndex) {
            print("\t\t. " + name + "$search_index" + ": creating JSON search index (manual refresh) ...");
            MetadataIndex.createJSONSearchIndex(name, pds);
        }

        final List<BSONFileConsumer> consumers = new ArrayList<>();

        if(dataFiles.size() > 0) {
            //println("\t- now loading data using " + cores + " parallel thread(s)");
            final BlockingQueue<List<byte[]>> queue = new LinkedBlockingQueue<>(cores == 1 ? 1 : cores - 1);

            final CountDownLatch producerCountDownLatch = new CountDownLatch(1);
            final CountDownLatch consumerCountDownLatch = new CountDownLatch(cores);

            // now load data!
            long producerStart = System.currentTimeMillis();
            new Thread(new BSONFileProducer(queue, dataFiles, cores, producerCountDownLatch, this)).start();

            long consumersStart = System.currentTimeMillis();
            for (int j = 0; j < cores; j++) {
                final BSONFileConsumer consumer = new BSONFileConsumer(j,name, queue, pds, consumerCountDownLatch, this, true);
                new Thread(consumer).start();
                consumers.add(consumer);
            }

            boolean producerDone = false;
            boolean consumersDone = false;

            final long TIMEOUT = 500L;

            try {

                while (!(producerDone && consumersDone)) {

                    //System.out.println("Waiting for producer...");
                    if (!producerDone) {
                        producerDone = producerCountDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                    }
                    //System.out.println("Waiting for consumers...");
                    if (!consumersDone) {
                        consumersDone = consumerCountDownLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
                    }

                    final long curTime = System.currentTimeMillis();
                    long producerDuration = (producerEndTime == 0 ? curTime : producerEndTime) - producerStart;

                    //System.out.println("producerDuration=" + producerEndTime + " / " + curTime + " / " + producerStart);

                    long consumersDuration = (consumersEndTime == 0 ? curTime : consumersEndTime) - consumersStart;
                    print(String.format("\t- read %d (%.0f d/s, %.1f MB/s) >> stored %d (%.0f d/s, %.1f MB/s)",
                            produced, 1000d * (double) produced / (double) producerDuration,
                            1000d * ((double) readSize / (1024.0 * 1024.0)) / (double) producerDuration,
                            consumed, 1000d * (double) consumed / (double) consumersDuration,
                            1000d * ((double) storedSize / (1024.0 * 1024.0)) / (double) consumersDuration));
                }
                println(String.format("\n  . Collection %s loaded with success (read %.1f MB, stored %.1f MB, comp. ratio x %.2f).",
                        name, (double) readSize / (1024.0 * 1024.0),
                        (double) storedSize / (1024.0 * 1024.0)
                        , (double) readSize / (double) storedSize));
            } catch (InterruptedException ignored) {
            }
        }

        // Merge consumers string values metadata (maxLengths)
        final Map<String,Integer> maxLengths = new HashMap<>();
        final Map<String,Set<String>> fieldsDataTypes = new HashMap<>();
        final Set<String> cantIndex = new HashSet<>();

        for(BSONFileConsumer bsonFileConsumer : consumers) {
            bsonFileConsumer.mergeMaxLengths(maxLengths);
            bsonFileConsumer.mergeFieldsDataTypes(fieldsDataTypes);
            bsonFileConsumer.mergeCantIndex(cantIndex);
        }

/*        for(String path:fieldsDataTypes.keySet()) {
            final Set<String> types = fieldsDataTypes.get(path);
            System.out.println(path + ":");
            for(String t:types) {
                System.out.println("- "+t);
            }
        }
*/
        // Manage indexes
        createAllIndexes(mustHaveASearchIndex,maxLengths,fieldsDataTypes,cantIndex,oracleMetadata);
    }

    private void createAllIndexes(boolean mustHaveASearchIndex, final Map<String, Integer> maxLengths, final Map<String, Set<String>> fieldsDataTypes, Set<String> cantIndex, Properties oracleMetadata) throws Exception {
        println(String.format("\t- found %d index(es)", mongoDBMetadata.getIndexes().length));

        for (MetadataIndex index : mongoDBMetadata.getIndexes()) {
            if ("_id_".equals(index.getName())) {
                println("\t\t. " + index.getName() + " (primary key, already exists)");
            } else if (index.getName().contains("$**")) {
                println("\t\t. " + index.getName() + ": (a JSON search index has already been created)");
            } else if (index.getKey().text) {
                println("\t\t. " + index.getName() + ": (a JSON search index has already been created)");
            } else if(index.getKey().spatial) {
                print("\t\t. " + index.getName() + " (spatial/2dsphere): creating it ...");
                index.createIndex(name, pds, maxLengths, fieldsDataTypes, cantIndex, oracleMetadata);
            } else {
                print("\t\t. " + index.getName() + ": creating it ...");
                index.createIndex(name, pds, maxLengths, fieldsDataTypes, cantIndex, oracleMetadata);
            }
        }

        if (mustHaveASearchIndex) {
            System.out.println("\t\t. " + name + "$search_index" + String.format(": you will need to synchronize it with:\n\t\t\tbegin\n\t\t\t\tCTX_DDL.SYNC_INDEX(idx_name => '%s$search_index', memory => '512M', parallel_degree => 6, locking => CTX_DDL.LOCK_NOWAIT);\n\t\t\tend;\n\t\t\t/",name));
        }
    }

    public static void main(String[] args) throws Throwable {
        JSONCollection j = new JSONCollection(args[0], new File(args[0]+".metadata.json"), null);
        j.loadMongoDBMetadataContent();
        j.createAllIndexes(false, new HashMap<>(), new HashMap<>(), new HashSet<>(), new Properties());
    }

    private void loadMongoDBMetadataContent() {
        final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        final SimpleModule indexKeyModule = new SimpleModule();
        indexKeyModule.addDeserializer(MetadataKey.class, new MetadataKeyDeserializer());
        mapper.registerModule(indexKeyModule);
        try (InputStream inputStream = metadata.getName().toLowerCase().endsWith(".gz") ?
                new GZIPInputStream(new FileInputStream(metadata), 16 * 1024)
                : new BufferedInputStream(new FileInputStream(metadata), 16 * 1024)) {
            mongoDBMetadata = mapper.readValue(inputStream, MongoDBMetadata.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createSODACollectionIfNotExists(final String name) throws Exception {
        final Properties props = new Properties();
        props.put("oracle.soda.sharedMetadataCache", "true");
        props.put("oracle.soda.localMetadataCache", "true");

        final OracleRDBMSClient cl = new OracleRDBMSClient(props);

        try (Connection c = pds.getConnection()) {
            OracleDatabase db = cl.getDatabase(c);

            OracleCollection oracleCollection = db.openCollection(name.toUpperCase());
            if (oracleCollection == null) {
                System.out.print("\t- creating SODA collection " + name + " ...");
                System.out.flush();

                // Underlying TABLE_NAME set to uppercase to ease later SQL queries!
                if (MongoDBBSONToAJDOSONLoading.KEEP_MONGODB_OBJECTIDS) {
                    db.admin().createCollection(name.toUpperCase(), db.createDocumentFromString(
                            "{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"CLIENT\"}," +
                                    "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"jsonFormat\":\"OSON\"}," +
                                    "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"UUID\"}," +
                                    "\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"}," +
                                    "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
                                    "\"readOnly\":false}"));
                } else {
                    // \"tableName\": \""+getProperTableName(name)+"\",
                    db.admin().createCollection(name.toUpperCase()/*,db.createDocumentFromString(
                            "{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
                                    "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"jsonFormat\":\"OSON\"}," +
                                    "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"UUID\"}," +
                                    "\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"}," +
                                    "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
                                    "\"readOnly\":false}")*/);
                }
                println(" OK");
            } else {
                // Verify the key assignment method according to MongoDB ObjectIds to keep or not
                final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                try {
                    final SODACollectionMetadata configuration = mapper.readValue(oracleCollection.admin().getMetadata().getContentAsString(), SODACollectionMetadata.class);
                    if (MongoDBBSONToAJDOSONLoading.KEEP_MONGODB_OBJECTIDS && !configuration.getKeyColumn().getAssignmentMethod().equalsIgnoreCase("client"))
                        throw new SODACollectionCantKeepMongoDBIdException(name);

                    if (!MongoDBBSONToAJDOSONLoading.KEEP_MONGODB_OBJECTIDS && configuration.getKeyColumn().getAssignmentMethod().equalsIgnoreCase("client"))
                        throw new SODACollectionIdNotAutomaticallyGeneratedException(name);
                } catch (IOException e) {
                    throw new SODACollectionMetadataUnmarshallException(e);
                }
            }
        }
    }


    //=== Used to track progress ===
    volatile String fileNameLoaded;

    @Override
    public void setFileNameLoaded(String fileNameLoaded) {
        this.fileNameLoaded = fileNameLoaded;
    }

    volatile long consumed = 0;
    volatile long readSize = 0;
    volatile long storedSize = 0;
    volatile long producerEndTime = 0;
    volatile long consumersEndTime = 0;

    @Override
    public void addConsumed(long number, long storedSize, boolean finished) {
        this.consumed += number;
        this.storedSize += storedSize;
        if (finished) {
            consumersEndTime = System.currentTimeMillis();
        }
    }

    volatile long produced = 0;

    @Override
    public void addProduced(long number, long readSize, boolean finished) {
        this.produced += number;
        this.readSize += readSize;
        if (finished) {
            producerEndTime = System.currentTimeMillis();
        }
    }

    public static String getProperTableName(String name) {
        name = name.toUpperCase();
        name = name.replace('.', '_');
        name = name.replace('-', '_');

        return name;
    }
}
