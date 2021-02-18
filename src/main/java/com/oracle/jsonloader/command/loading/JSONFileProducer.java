package com.oracle.jsonloader.command.loading;

import com.oracle.jsonloader.util.BlockingQueueCallback;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;

public class JSONFileProducer implements Runnable {
    private final BlockingQueue<List<String>> queue;
    private final List<File> dataFiles;
    private final int numberOfConsumers;
    private final CountDownLatch countDownLatch;
    private final BlockingQueueCallback callback;

    public JSONFileProducer(BlockingQueue<List<String>> queue, List<File> dataFiles,
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
            readFileAndProduceJSONs();

            countDownLatch.countDown();
        } catch (Exception e) {
            //e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private void readFileAndProduceJSONs() throws InterruptedException, IOException {

        for (int i = 0; i < dataFiles.size(); i++) {
            final File f = dataFiles.get(i);

            callback.setFileNameLoaded(f.getName());

            try (
                    BufferedReader reader = new BufferedReader(new InputStreamReader(f.getName().toLowerCase().endsWith(".gz") ?
                            new GZIPInputStream(new FileInputStream(f), 16 * 1024 * 1024)
                            : new BufferedInputStream(new FileInputStream(f), 16 * 1024 * 1024), StandardCharsets.UTF_8), 16*1024*1024)
            ) {
                List<String> batch = new ArrayList<>();

                long readSize = 0;
                while (true) {
                    try {
                        final String line = reader.readLine();
                        if(line == null) throw new EOFException();
                        readSize += line.getBytes().length;
                        batch.add(line);
                    } catch (EOFException eof) {
                        // Managing end of file!
                        //System.out.println("Passing a batch of "+batch.size()+" bsons");
                        if (batch.size() > 0) {
                            queue.put(batch);
                        }

                        if (i == dataFiles.size() - 1) {
                            callback.addProduced(batch.size(), readSize, true);
                            for (int nOC = 0; nOC < numberOfConsumers; nOC++) queue.put(new ArrayList<>());
                        } else {
                            callback.addProduced(batch.size(), readSize, false);
                        }

                        break;
                    }
                    catch (Throwable t) {
                        t.printStackTrace();
                    }

                    if (batch.size() == Load.BATCH_SIZE) {
                        //System.out.println("Passing a batch of "+batch.size()+" jsons");
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
