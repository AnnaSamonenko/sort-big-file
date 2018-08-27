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

    private int sizeInMB = 100;
    private static int amountOfThreads;
    private LinkedBlockingDeque<List<String>> blockingQueue = new LinkedBlockingDeque<>();
    private Semaphore semaphore = new Semaphore(2);
    private BufferedReader br;
    private double amountOfFiles;
    private double amountOfStringsPerFile;
    private AtomicBoolean flag = new AtomicBoolean(true);
    private CountDownLatch countDownLatch;


    public FileHelperInParallel(String pathToBigFile) throws IOException {
        File unsortedFile;
        br = new BufferedReader(new FileReader(pathToBigFile));
        unsortedFile = new File(pathToBigFile);
        amountOfFiles = Math.ceil(FileHelper.convertBytesInMB(unsortedFile.length()) / sizeInMB);
        amountOfStringsPerFile = Math.ceil(FileHelper.countAmountOfLines(pathToBigFile) /
                amountOfFiles);
        long freeMemoryInBytes = Runtime.getRuntime().maxMemory();
        amountOfThreads = (int) FileHelper.convertBytesInMB(freeMemoryInBytes) / 300;
        countDownLatch = new CountDownLatch(amountOfThreads);
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
                consumer(pathToPartsOfFile, "file");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        (new Thread(producer)).start();
        for (int i = 0; i < amountOfThreads; i++)
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

    private void consumer(String pathToParts, String fileName) throws InterruptedException {
        while (flag.get() || !blockingQueue.isEmpty()) {
            semaphore.acquire();
            if (!blockingQueue.isEmpty()) {
                try {
                    List<String> list = blockingQueue.takeLast();
                    Collections.sort(list);
                    Path out = Paths.get(pathToParts + "/" + fileName + (new Random().nextInt()));
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
