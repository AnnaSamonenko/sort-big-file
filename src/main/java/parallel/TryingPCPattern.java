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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

public class TryingPCPattern implements AutoCloseable {

    private static int sizeInMB = 100;
    private LinkedBlockingDeque<List<String>> blockingQueue = new LinkedBlockingDeque<>(10);
    private CountDownLatch countDownLatch;
    private BufferedReader br;
    File unsortedFile;
    double amountOfFiles;
    double amountOfStringsPerFile;


    public TryingPCPattern(String pathToBigFile) throws IOException {
        br = new BufferedReader(new FileReader(pathToBigFile));
        unsortedFile = new File(pathToBigFile);
        amountOfFiles = Math.ceil(FileHelper.convertBytesInMB(unsortedFile.length()) / sizeInMB);
        amountOfStringsPerFile = Math.ceil(FileHelper.countAmountOfLine(pathToBigFile) /
                amountOfFiles);
    }

    @Override
    public void close() throws IOException {
        br.close();
    }

    public void runDivideAndSortInParallel(String pathToPartsOfFile, String pathToUnsortedFile, int sizeInMB) throws IOException {
        Files.createDirectory(Paths.get(pathToPartsOfFile));

        Thread th1 = new Thread(new Runnable() {
            @Override
            public void run() {
                producer();
            }
        }).start();

        Thread th2 = new Thread(new Runnable() {
            @Override
            public void run() {
                consumer(pathToPartsOfFile, "file");
            }
        }).start();

    }

    private void producer() {
        try {
            String line;
            List<String> list = new ArrayList<>();

            for (int j = 0; j < amountOfStringsPerFile; j++) {
                line = br.readLine();
                if (line == null)
                    break;
                list.add(line);
            }
            blockingQueue.add(list);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    private void consumer(String pathToParts, String fileName) {
        try {
            List<String> list = blockingQueue.takeLast();
            Collections.sort(list);
            Path out = Paths.get(pathToParts + "/" + fileName);
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
