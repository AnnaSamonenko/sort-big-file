package merge;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class MergeHelper implements AutoCloseable {
    private FileWriter fw;
    private final List<Wrapper> wrappers = new ArrayList<>();
    private static final int AMOUNT_OF_THREADS = 2;
    private CountDownLatch countDownLatch = new CountDownLatch(AMOUNT_OF_THREADS);
    private AtomicBoolean flag = new AtomicBoolean(true);
    private final LinkedList<String> buffer = new LinkedList<>();

    public MergeHelper(String dirPath, String fileName) {
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

        File[] dir = (new File(pathToDir)).listFiles();
        for (File file : dir)
            wrappers.add(new Wrapper(file));

        // producer
        Runnable producer = () -> {
            producer();
        };

        // consumer
        Runnable consumer = () -> {
            try {
                consumer();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        };

        (new Thread(producer)).start();
        (new Thread(consumer)).start();
        return countDownLatch;
    }

    private void producer() {
        Comparator<Wrapper> wrappersLineComparator = Comparator.comparing(Wrapper::getLine);
        while (flag.get()) {
            Collections.sort(wrappers, wrappersLineComparator);
            Wrapper wr = wrappers.get(0);
            synchronized (buffer) {
                buffer.add(wr.getLine());
            }
            wr.nextLine();
            if (wr.getLine() == null) {
                wr.close();
                wrappers.remove(0);
            }

            if (wrappers.isEmpty())
                flag.set(false);
        }
        countDownLatch.countDown();
    }

    private void consumer() throws IOException {
        List<String> temp = new ArrayList<>();
        while (flag.get()) {
            synchronized (buffer) {
                if (buffer.size() >= 1000) {
                    temp.addAll(buffer);
                    buffer.clear();
                }
            }
            if (!temp.isEmpty())
                writeToFile(temp);
            temp.clear();
        }

        if (!buffer.isEmpty()) {
            writeToFile(buffer);
        }

        countDownLatch.countDown();
    }

    private void writeToFile(List<String> lines) throws IOException {
        for (String line : lines)
            writeToFile(line);
    }

    private void writeToFile(String line) throws IOException {
        fw.write(line + System.getProperty("line.separator"));
    }

    @Override
    public void close() throws IOException {
        fw.close();
    }
}
