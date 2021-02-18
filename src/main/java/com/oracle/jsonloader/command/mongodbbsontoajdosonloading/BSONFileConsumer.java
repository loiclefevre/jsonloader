package com.oracle.jsonloader.command.mongodbbsontoajdosonloading;

import com.oracle.jsonloader.util.BlockingQueueCallback;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;
import org.bson.BSONObject;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class BSONFileConsumer implements Runnable {

    private final BlockingQueue<List<byte[]>> queue;
    private final PoolDataSource pds;
    private final String collectionName;
    private final CountDownLatch consumerCountDownLatch;
    private final BlockingQueueCallback callback;
    private final boolean outputOsonFormat;
    private final int id;
    private MyBSONDecoder decoder;

    public BSONFileConsumer(int id, String collectionName, BlockingQueue<List<byte[]>> queue,
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
            decoder = new MyBSONDecoderWithMetadata(outputOsonFormat);
            long count = 0;
            long bsonSize = 0;
            long osonSize = 0;

            try (Connection c = pds.getConnection()) {
                c.setAutoCommit(false);

                //========================================================
                // JDBC START
//                try (PreparedStatement p = c.prepareStatement(
//                        "insert /*+ append */ into " + collectionName + " (ID,JSON_DOCUMENT,VERSION) values (?,?,'1')")) {
//                    while (true) {
//                        final List<byte[]> batch = queue.take();
//                        if (batch.size() == 0) {
//                            callback.addConsumed(0, 0, true);
//                            break;
//                        }
//
//                        //System.out.println("Treating "+batch.size()+" docs");
//
//                        for (byte[] data : batch) {
//                            final BSONObject obj = decoder.readObject(data);
//                            if (obj != null) {
//                                count++;
//                                bsonSize += decoder.getBsonLength();
//                                final byte[] osonData = decoder.getOSONData();
//                                osonSize += osonData.length;
//
//                                p.setString(1, decoder.getOid());
//                                p.setBytes(2, osonData);
//                                p.addBatch();
//                            }
//                        }
//
//                        //System.out.println("Running batch...("+osonSize+")");
//
//                        p.executeLargeBatch();
//                        c.commit();
//
//                        callback.addConsumed(batch.size(),osonSize);
//                        osonSize = 0;
//                    }
//                }
                // JDBC END
                //========================================================
                // SODA START

                final Properties props = new Properties();
                props.put("oracle.soda.sharedMetadataCache", "true");
                props.put("oracle.soda.localMetadataCache", "true");

                final OracleRDBMSClient cl = new OracleRDBMSClient(props);
                final OracleDatabase db = cl.getDatabase(c);
                final OracleCollection collection = db.openCollection(collectionName.toUpperCase());

                //try (PreparedStatement p = c.prepareStatement("insert /*+ append */ into " + collectionName + " (ID,JSON_DOCUMENT,VERSION) values (?,?,'1')")) {
                final List<OracleDocument> batchDocuments = new ArrayList<>(MongoDBBSONToAJDOSONLoading.BATCH_SIZE);

                final Map<String, String> insertOptions = new HashMap<>();
                insertOptions.put("hint", "append");

                if (MongoDBBSONToAJDOSONLoading.KEEP_MONGODB_OBJECTIDS) {
                    while (true) {
                        final List<byte[]> batch = queue.take();
                        if (batch.size() == 0) {
                            callback.addConsumed(0, 0, true);
                            break;
                        }

                        //System.out.println("Treating "+batch.size()+" docs");

                        for (byte[] data : batch) {
                            final BSONObject obj = decoder.readObject(data);
                            if (obj != null) {
                                count++;

                                bsonSize += decoder.getBsonLength();
                                final byte[] osonData = decoder.getOSONData();
                                osonSize += osonData.length;

                                //System.out.println("ObjectId: "+decoder.getOid()+", BSON: "+decoder.getBsonLength()+", OSON: "+osonData.length);

                                batchDocuments.add(db.createDocumentFrom(decoder.getOid(), osonData));

                                //p.setString(1, decoder.getOid());
                                //p.setBytes(2, osonData);
                                //p.addBatch();
                            }
                        }

                        //System.out.println("Running batch...("+osonSize+")");

                        //p.executeLargeBatch();
                        collection.insertAndGet(batchDocuments.iterator(), insertOptions);

                        batchDocuments.clear();
                        c.commit();

                        callback.addConsumed(batch.size(), osonSize, false);
                        osonSize = 0;
                    }
                } else {
                    while (true) {
                        final List<byte[]> batch = queue.take();
                        if (batch.size() == 0) {
                            callback.addConsumed(0, 0, true);
                            break;
                        }

                        //System.out.println("Treating "+batch.size()+" docs");

                        for (byte[] data : batch) {
                            final BSONObject obj = decoder.readObject(data);
                            if (obj != null) {
                                count++;

                                bsonSize += decoder.getBsonLength();
                                final byte[] osonData = decoder.getOSONData();
                                osonSize += osonData.length;


                                batchDocuments.add(db.createDocumentFrom(osonData));

                                //p.setString(1, decoder.getOid());
                                //p.setBytes(2, osonData);
                                //p.addBatch();
                            }
                        }

                        //System.out.println("Running batch...("+osonSize+")");

                        //p.executeLargeBatch();
                        collection.insertAndGet(batchDocuments.iterator(), insertOptions);

                        batchDocuments.clear();
                        c.commit();

                        callback.addConsumed(batch.size(), osonSize, false);
                        osonSize = 0;
                    }
                }
                //}
                // SODA END
                //========================================================
            }
        } catch (Exception e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
            consumerCountDownLatch.countDown();
        }

    }

    public void mergeMaxLengths(Map<String, Integer> aggregatedMaxLengths) {
        final Map<String, Integer> maxLengths = ((MyBSONDecoderWithMetadata) this.decoder).getMaxLengths();
        for (String key : maxLengths.keySet()) {
            if (aggregatedMaxLengths.containsKey(key)) {
                aggregatedMaxLengths.put(key, Math.max(maxLengths.get(key), aggregatedMaxLengths.get(key)));
            } else {
                aggregatedMaxLengths.put(key, maxLengths.get(key));
            }
        }
    }

    public void mergeFieldsDataTypes(Map<String, Set<String>> aggregatedFieldsDataTypes) {
        final Map<String, Set<String>> fieldsDataTypes = ((MyBSONDecoderWithMetadata) this.decoder).getFieldsDataTypes();
        for (String key : fieldsDataTypes.keySet()) {
            if (aggregatedFieldsDataTypes.containsKey(key)) {
                aggregatedFieldsDataTypes.get(key).addAll(fieldsDataTypes.get(key));
            } else {
                aggregatedFieldsDataTypes.put(key, fieldsDataTypes.get(key));
            }
        }
    }

    public void mergeCantIndex(Set<String> aggregatedCantIndex) {
        aggregatedCantIndex.addAll(((MyBSONDecoderWithMetadata) this.decoder).getCantIndex());
    }
}
