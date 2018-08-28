package util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class MergeOfFiles implements AutoCloseable {
    private FileWriter fw;
    private CopyOnWriteArrayList<Wrapper> wrappers = new CopyOnWriteArrayList<>();
    private static final int AMOUNT_OF_THREADS = 2;
    private CountDownLatch countDownLatch = new CountDownLatch(AMOUNT_OF_THREADS);

    public MergeOfFiles(String dirPath, String fileName) {
        try {
            if (!(new File(dirPath).exists())) {
                Files.createDirectory(Paths.get(dirPath));
            } else {
                File file = new File(dirPath);
                if (file.list().length != 0) {
                    File[] files = file.listFiles();
                    for (File f : files)
                        f.delete();
                }
            }
            Files.createFile(Paths.get(dirPath + fileName));

            fw = new FileWriter(dirPath + fileName, true);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public CountDownLatch mergeInParallel(String pathToDir) {

        // producer
        Runnable producer = () ->
                producer(pathToDir);

        // consumer
        Runnable consumer = () ->
                consumer();

        (new Thread(producer)).start();
        (new Thread(consumer)).start();

        return countDownLatch;
    }

    private void producer(String pathToDir) {
        File[] dir = (new File(pathToDir)).listFiles();
        for (File file : dir)
            wrappers.add(new Wrapper(file));
        countDownLatch.countDown();
    }

    private void consumer() {
        Comparator<Wrapper> wrappersLineComparator = Comparator.comparing(Wrapper::getLine);

        while (!wrappers.isEmpty()) {
            Collections.sort(wrappers, wrappersLineComparator);

            Wrapper wr = wrappers.get(0);
            try {
                writeToFile(wr.getLine());
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            wr.nextLine();
            if (wr.getLine() == null) {
                wr.close();
                wrappers.remove(0);
            }
        }
        countDownLatch.countDown();
    }

    private void writeToFile(String line) throws IOException {
        fw.write(line + System.getProperty("line.separator"));
    }

    @Override
    public void close() throws IOException {
        fw.close();
    }
}
