package parallel;

import helper.FileHelper;

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
import java.util.concurrent.atomic.AtomicBoolean;

public class TryingPCPattern implements AutoCloseable {

    private static int sizeInMB = 100;
    private LinkedBlockingDeque<List<String>> blockingQueue = new LinkedBlockingDeque<>(3);
    private CountDownLatch countDownLatch;
    private BufferedReader br;
    double amountOfFiles;
    double amountOfStringsPerFile;
    AtomicBoolean flag = new AtomicBoolean(true);


    public TryingPCPattern(String pathToBigFile) throws IOException {
        File unsortedFile;
        br = new BufferedReader(new FileReader(pathToBigFile));
        unsortedFile = new File(pathToBigFile);
        amountOfFiles = Math.ceil(FileHelper.convertBytesInMB(unsortedFile.length()) / sizeInMB);
        amountOfStringsPerFile = Math.ceil(FileHelper.countAmountOfLine(pathToBigFile) /
                amountOfFiles);
    }

    @Override
    public void close() throws IOException {
        // br.close();
    }

    public void runDivideAndSortInParallel(String pathToPartsOfFile) throws IOException {
        Files.createDirectory(Paths.get(pathToPartsOfFile));

        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                producer();
            }
        });

        Thread th2 = new Thread(new Runnable() {
            @Override
            public void run() {
                consumer(pathToPartsOfFile, "file");
            }
        });

        th1.start();
        th2.start();
    }

    private void producer() {
        while (flag.get()) {
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
    }

    private void consumer(String pathToParts, String fileName) {
        while (flag.get()) {
            try {
                List<String> list = blockingQueue.takeLast();
                Collections.sort(list);
                Path out = Paths.get(pathToParts + "/" + fileName + (new Random().nextInt()));
                try {
                    Files.write(out, list, Charset.defaultCharset());
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }
}
