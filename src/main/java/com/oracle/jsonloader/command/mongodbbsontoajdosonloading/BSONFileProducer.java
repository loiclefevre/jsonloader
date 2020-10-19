package com.oracle.jsonloader.command.mongodbbsontoajdosonloading;

import com.oracle.jsonloader.command.mongodbbsontoajdosonloading.BlockingQueueCallback;
import com.oracle.jsonloader.command.mongodbbsontoajdosonloading.JSONCollection;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;

public class BSONFileProducer implements Runnable {
    private final BlockingQueue<List<byte[]>> queue;
    private final List<File> dataFiles;
    private final int numberOfConsumers;
    private final CountDownLatch countDownLatch;
    private final BlockingQueueCallback callback;

    public BSONFileProducer(BlockingQueue<List<byte[]>> queue, List<File> dataFiles,
                            int numberOfConsumers, CountDownLatch countDownLatch, BlockingQueueCallback callback) {
        this.queue = queue;
        this.dataFiles = dataFiles;
        this.numberOfConsumers = numberOfConsumers;
        this.countDownLatch = countDownLatch;
        this.callback = callback;
    }

    @Override
    public void run() {
        try {
            readFileAndProduceBSONs();

            countDownLatch.countDown();
        } catch (Exception e) {
            //e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private final byte[] bsonDataSize = new byte[4];

    private byte[] readNextBSONRawData(InputStream input) throws IOException {
        int readBytes = input.read(bsonDataSize, 0, 4);
        if (readBytes != 4) throw new EOFException();

        final int bsonSize = (bsonDataSize[0] & 0xff) |
                ((bsonDataSize[1] & 0xff) << 8) |
                ((bsonDataSize[2] & 0xff) << 16) |
                ((bsonDataSize[3] & 0xff) << 24);

        final byte[] rawData = new byte[bsonSize];

        System.arraycopy(bsonDataSize, 0, rawData, 0, 4);

        for (int i = bsonSize - 4, off = 4; i > 0; off += readBytes) {
            readBytes = input.read(rawData, off, i);
            if (readBytes < 0) {
                throw new EOFException();
            }

            i -= readBytes;
        }

        return rawData;
    }

    private void readFileAndProduceBSONs() throws InterruptedException, IOException {
        for (File f : dataFiles) {
            try (
                    InputStream inputStream = f.getName().toLowerCase().endsWith(".gz") ?
                            new GZIPInputStream(new FileInputStream(f), 16 * 1024 * 1024)
                            : new BufferedInputStream(new FileInputStream(f), 16 * 1024 * 1024)
            ) {
                List<byte[]> batch = new ArrayList<>();

                long readSize = 0;
                while (true) {
                    try {
                        final byte[] data = readNextBSONRawData(inputStream);
                        readSize += data.length;
                        batch.add(data);
                    } catch (EOFException eof) {
                        // Managing end of file!
                        if (batch.size() > 0) {
                            //System.out.println("Passing a batch of "+batch.size()+" bsons");
                            queue.put(batch);
                            callback.addProduced(batch.size(), readSize, true);
                            for (int i = 0; i < numberOfConsumers; i++) queue.put(new ArrayList<>());
                        }

                        break;
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }

                    if (batch.size() == MongoDBBSONToAJDOSONLoading.BATCH_SIZE) {
                        //System.out.println("Passing a batch of "+batch.size()+" bsons");
                        queue.put(batch);
                        callback.addProduced(batch.size(), readSize, false);
                        batch = new ArrayList<>();
                        readSize = 0;
                    }
                }
            }
        }
    }
}
