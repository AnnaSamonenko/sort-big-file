package helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileHelperInParallel implements AutoCloseable {

    private LinkedBlockingDeque<List<String>> blockingQueue = new LinkedBlockingDeque<>(3);

    private static final int SIZE_OF_SMALL_FILES_IN_MB = 100;
    private static final int AMOUNT_OF_CONSUMER_THREADS = 3;

    private Semaphore semaphore = new Semaphore(2);
    private CountDownLatch countDownLatch = new CountDownLatch(AMOUNT_OF_CONSUMER_THREADS + 1);
    private AtomicBoolean flag = new AtomicBoolean(true);

    private BufferedReader br;
    private double amountOfStringsPerFile;

    public FileHelperInParallel(String pathToBigFile) throws IOException {
        br = new BufferedReader(new FileReader(pathToBigFile));
        double amountOfFiles = Math.ceil(FileHelper.convertBytesInMB((new File(pathToBigFile)).length()) / SIZE_OF_SMALL_FILES_IN_MB);
        amountOfStringsPerFile = Math.ceil(FileHelper.countAmountOfLines(pathToBigFile) /
                amountOfFiles);
    }

    public CountDownLatch runDivideAndSortInParallel(String pathToPartsOfFile) throws IOException {
        Files.createDirectory(Paths.get(pathToPartsOfFile));

        Runnable producer = () -> {
            try {
                producer();
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        };

        Runnable consumer = () -> {
            try {
                consumer(pathToPartsOfFile);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        (new Thread(producer)).start();
        for (int i = 0; i < AMOUNT_OF_CONSUMER_THREADS; i++)
            (new Thread(consumer)).start();

        return countDownLatch;
    }

    private void producer() throws InterruptedException {
        while (flag.get()) {
            semaphore.acquire();
            if (blockingQueue.size() < 2) {
                try {
                    String line;
                    List<String> list = new ArrayList<>();
                    for (int j = 0; j < amountOfStringsPerFile; j++) {
                        line = br.readLine();
                        if (line == null) {
                            flag.set(false);
                            break;
                        }
                        list.add(line);
                    }
                    blockingQueue.add(list);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            semaphore.release();
        }
        countDownLatch.countDown();
    }

    private void consumer(String pathToParts) throws InterruptedException {
        while (flag.get() || !blockingQueue.isEmpty()) {
            semaphore.acquire();
            if (!blockingQueue.isEmpty()) {
                try {
                    List<String> list = blockingQueue.takeLast();
                    Collections.sort(list);
                    Path out = Paths.get(pathToParts + "/" + "file" + (new Random().nextInt()));
                    Files.write(out, list, Charset.defaultCharset());
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            semaphore.release();
        }
        countDownLatch.countDown();
    }

    @Override
    public void close() throws IOException {
        br.close();
    }
}
