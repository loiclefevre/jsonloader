package com.oracle.jsonloader.command.loading;

import com.oracle.jsonloader.exception.*;
import oracle.jdbc.OracleConnection;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import static com.oracle.jsonloader.util.Console.print;
import static com.oracle.jsonloader.util.Console.println;

public class Load {
	public static void main(final String[] args) throws JSONLoaderException {
		new Load(args).run();
	}

	public static int BATCH_SIZE = 5000;

	private String sourceDir;
	private String ajdConnectionService;
	private String user;
	private String password;
	private String collectionName;
	private int specificCores = -1;

	public Load(String[] args) throws JSONLoaderException {
		analyzeCLIParameters(args);
	}

	private void run() throws JSONLoaderException {
		final File dataDir = new File(sourceDir);

		if (!dataDir.exists() && dataDir.isDirectory()) {
			throw new SourceDirectoryNotFoundException(dataDir.getAbsolutePath());
		}

		println("================================================================================");
		int cores = Runtime.getRuntime().availableProcessors();
		if (cores > 1) {
			cores--;
		}

		if(specificCores <= 0) {
			specificCores = cores;
		}

		PoolDataSource pds = null;
		try {
			print("Initializing connection pool ...");
			pds = initializeConnectionPool(ajdConnectionService, user, password, specificCores);

			//test(pds);

			println(" OK");

			println("Loading collection " + collectionName+"...");

			final JSONCollectionForLoading collection = new JSONCollectionForLoading(collectionName, dataDir, pds);

			// Loading all collections
			collection.findDatafiles();
			collection.load(specificCores);

		} catch (Throwable t) {
			throw new UnknownException(ErrorCode.Unknown, "Unhandled error", t);
		}
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
		if (args.length < 5) {
			throw new BadCLIUsageException();
		}

		for (int i = 1; i < args.length; i++) {
			switch (i) {
				case 1:
					sourceDir = args[i];
					break;

				case 2:
					ajdConnectionService = args[i];
					break;

				case 3:
					user = args[i];
					break;

				case 4:
					password = args[i];
					break;

				case 5:
					collectionName = args[i];
					break;

				case 6:
					specificCores = Integer.parseInt(args[i]);
					break;

				case 7:
					BATCH_SIZE = Integer.parseInt(args[i]);
					break;

			}
		}

		println("> Data files directory          : " + sourceDir);
		println("> Destination database service  : " + ajdConnectionService);
		println("> Destination database user name: " + user);
		if(specificCores > 0) {
		  println("> Parallel degree               : " + specificCores);
		}
		println("> JSON loading batch size       : " + BATCH_SIZE);

	}

}
