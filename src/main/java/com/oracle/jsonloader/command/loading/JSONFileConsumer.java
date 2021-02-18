package com.oracle.jsonloader.command.loading;

import com.oracle.jsonloader.util.BlockingQueueCallback;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class JSONFileConsumer implements Runnable {

	private final BlockingQueue<List<String>> queue;
	private final PoolDataSource pds;
	private final String collectionName;
	private final CountDownLatch consumerCountDownLatch;
	private final BlockingQueueCallback callback;
	private final boolean outputOsonFormat;
	private final int id;
	private MyJSONDecoder decoder;

	public JSONFileConsumer(int id, String collectionName, BlockingQueue<List<String>> queue,
							PoolDataSource pds, CountDownLatch consumerCountDownLatch,
							BlockingQueueCallback callback, boolean outputOsonFormat) {
		this.id = id;
		this.collectionName = collectionName;
		this.queue = queue;
		this.pds = pds;
		this.consumerCountDownLatch = consumerCountDownLatch;
		this.callback = callback;
		this.outputOsonFormat = outputOsonFormat;
	}

	public void run() {
		try {
			//decoder = (id == 0 ? new MyBSONDecoderWithMetadata(outputOsonFormat) : new MyBSONDecoder(outputOsonFormat));
			decoder = new MyJSONDecoder(outputOsonFormat);
			long count = 0;
			long jsonSize = 0;
			long osonSize = 0;

			try (Connection c = pds.getConnection()) {
				c.setAutoCommit(false);

				//========================================================
				// SODA START

				final Properties props = new Properties();
				props.put("oracle.soda.sharedMetadataCache", "true");
				props.put("oracle.soda.localMetadataCache", "true");

				final OracleRDBMSClient cl = new OracleRDBMSClient(props);
				final OracleDatabase db = cl.getDatabase(c);
				final OracleCollection collection = db.openCollection(collectionName.toUpperCase());

				final List<OracleDocument> batchDocuments = new ArrayList<>(Load.BATCH_SIZE);

				final Map<String, String> insertOptions = new HashMap<>();
				insertOptions.put("hint", "append");

				while (true) {
					final List<String> batch = queue.take();
					if (batch.size() == 0) {
						callback.addConsumed(0, 0, true);
						break;
					}

					//System.out.println("Treating "+batch.size()+" docs");

					for (String data : batch) {
						decoder.readObject(data);
						count++;

						jsonSize += decoder.getLength();
						final byte[] osonData = decoder.getOSONData();
						osonSize += osonData.length;

						batchDocuments.add(db.createDocumentFrom(osonData));
					}

					//System.out.println("Running batch...("+osonSize+")");

					//p.executeLargeBatch();
					collection.insertAndGet(batchDocuments.iterator(), insertOptions);

					batchDocuments.clear();
					c.commit();

					callback.addConsumed(batch.size(), osonSize, false);
					osonSize = 0;
				}

				//}
				// SODA END
				//========================================================
			}
		} catch (Exception e) {
			//e.printStackTrace();
			Thread.currentThread().interrupt();
		} finally {
			consumerCountDownLatch.countDown();
		}

	}
}
