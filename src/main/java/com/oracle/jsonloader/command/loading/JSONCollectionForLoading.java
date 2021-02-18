package com.oracle.jsonloader.command.loading;

import com.oracle.jsonloader.util.BlockingQueueCallback;
import com.oracle.jsonloader.util.JSONDataFilenameFilter;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.oracle.jsonloader.util.Console.print;
import static com.oracle.jsonloader.util.Console.println;

public class JSONCollectionForLoading implements BlockingQueueCallback {
	public final String name;
	public final File sourceDir;
	public final List<File> dataFiles = new ArrayList<>();
	private PoolDataSource pds;

	public JSONCollectionForLoading(final String name, final File sourceDir, final PoolDataSource pds) {
		this.name = name;
		this.sourceDir = sourceDir;
		this.pds = pds;
	}

	/**
	 * Discover data files associated with this collection using either .bson or .bson.gz file extension.
	 */
	public void findDatafiles() {
		long totalSize = 0;

		for (File f : sourceDir.listFiles(new JSONDataFilenameFilter())) {
			if (f.isFile() && f.length() > 0) {
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
	public void load(int cores) throws Exception {
		// ensure collection exists!
		createSODACollectionIfNotExists(name);

		final List<JSONFileConsumer> consumers = new ArrayList<>();

		if (dataFiles.size() > 0) {
			//println("\t- now loading data using " + cores + " parallel thread(s)");
			final BlockingQueue<List<String>> queue = new LinkedBlockingQueue<>(cores == 1 ? 1 : cores - 1);

			final CountDownLatch producerCountDownLatch = new CountDownLatch(1);
			final CountDownLatch consumerCountDownLatch = new CountDownLatch(cores);

			// now load data!
			long producerStart = System.currentTimeMillis();
			new Thread(new JSONFileProducer(queue, dataFiles, cores, producerCountDownLatch, this)).start();

			long consumersStart = System.currentTimeMillis();
			for (int j = 0; j < cores; j++) {
				final JSONFileConsumer consumer = new JSONFileConsumer(j, name, queue, pds, consumerCountDownLatch, this, true);
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
					print(String.format("\t- [%s] read %d (%.0f d/s, %.1f MB/s) >> stored %d (%.0f d/s, %.1f MB/s, x %.2f)",
							fileNameLoaded, produced, 1000d * (double) produced / (double) producerDuration,
							1000d * ((double) readSize / (1024.0 * 1024.0)) / (double) producerDuration,
							consumed, 1000d * (double) consumed / (double) consumersDuration,
							1000d * ((double) storedSize / (1024.0 * 1024.0)) / (double) consumersDuration,
							(double) readSize / (double) storedSize));
				}
				println(String.format("\n  . Collection %s loaded with success (read %.1f MB, stored %.1f MB, comp. ratio x %.2f).",
						name, (double) readSize / (1024.0 * 1024.0),
						(double) storedSize / (1024.0 * 1024.0)
						, (double) readSize / (double) storedSize));
			} catch (InterruptedException ignored) {
			}
		}
	}


	public static void main(String[] args) throws Throwable {
		JSONCollectionForLoading j = new JSONCollectionForLoading(args[0], new File(args[0] + ".json"), null);
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


				// \"tableName\": \""+getProperTableName(name)+"\",
				db.admin().createCollection(name.toUpperCase()/*,db.createDocumentFromString(
                            "{\"keyColumn\":{\"name\":\"ID\",\"sqlType\":\"VARCHAR2\",\"maxLength\":255,\"assignmentMethod\":\"UUID\"}," +
                                    "\"contentColumn\":{\"name\":\"JSON_DOCUMENT\",\"sqlType\":\"BLOB\",\"jsonFormat\":\"OSON\"}," +
                                    "\"versionColumn\":{\"name\":\"VERSION\",\"type\":\"String\",\"method\":\"UUID\"}," +
                                    "\"lastModifiedColumn\":{\"name\":\"LAST_MODIFIED\"}," +
                                    "\"creationTimeColumn\":{\"name\":\"CREATED_ON\"}," +
                                    "\"readOnly\":false}")*/);
				println(" OK");
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
