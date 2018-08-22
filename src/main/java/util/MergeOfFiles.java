package util;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class MergeOfFiles implements AutoCloseable {
    private FileWriter fw;

    public MergeOfFiles(String dirPath, String fileName) {
        try {
            Files.createDirectory(Paths.get(dirPath));
            Files.createFile(Paths.get(dirPath + fileName));
            fw = new FileWriter(dirPath + fileName, true);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public void merge(String pathToDir) throws IOException {
        File[] dir = (new File(pathToDir)).listFiles();
        List<Wrapper> wrappers = new ArrayList<>();

        for (File file : dir)
            wrappers.add(new Wrapper(file));

        Comparator<Wrapper> wrappersLineComparator = Comparator.comparing(Wrapper::getLine);

        while (!wrappers.isEmpty()) {

            Collections.sort(wrappers, wrappersLineComparator);

            Wrapper wr = wrappers.get(0);
            writeToFile(wr.getLine());
            wr.nextLine();
            if (wr.getLine() == null) {
                wr.close();
                wrappers.remove(0);
            }
        }
    }

    private void writeToFile(String line) throws IOException {
        fw.write(line + System.getProperty("line.separator"));
    }

    @Override
    public void close() throws IOException {
        fw.close();
    }
}
