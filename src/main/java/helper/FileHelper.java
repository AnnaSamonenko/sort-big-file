package helper;

import util.StringGenerator;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class FileHelper {
    private static final int LENGTH_OF_STRING = 32;
    private static final int MEGABYTE = 1024 * 1024;

    private FileHelper() {
    }

    // standalone big file generator
    public static void generateBigFile(String path, String fileName, int sizeInMB) throws IOException {
        Files.createDirectory(Paths.get(path));
        Files.createFile(Paths.get(path + fileName));
        File file = new File(path + fileName);

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file, true))) {
            while ((file.length() / MEGABYTE) < sizeInMB) {
                bw.write(StringGenerator.randomString(LENGTH_OF_STRING));
                bw.newLine();
            }
        }
    }

    // divide big file on small files
    public static void divideAndSortFile(String pathToUnsortedFile, String pathToPartsOfFile, double sizeInMB)
            throws IOException {
        File unsortedFile = new File(pathToUnsortedFile);
        Files.createDirectory(Paths.get(pathToPartsOfFile));
        String line;
        List<String> list;
        double amountOfFiles = Math.ceil(convertBytesInMB(unsortedFile.length()) / sizeInMB);
        double amountOfStringsPerFile = Math.ceil(countAmountOfLine(pathToUnsortedFile) /
                amountOfFiles);

        try (BufferedReader br = new BufferedReader(new FileReader(unsortedFile))) {
            for (int i = 0; i < amountOfFiles; i++) {
                File file = new File(pathToPartsOfFile + "file" + i + ".txt");
                list = new ArrayList<>();
                Files.createFile(Paths.get(file.getPath()));
                file.getParentFile().deleteOnExit();
                file.deleteOnExit();

                for (int j = 0; j < amountOfStringsPerFile; j++) {
                    line = br.readLine();
                    if (line == null)
                        break;
                    list.add(line);
                }
                Collections.sort(list);
                writeToFile(file.getPath(), list);
            }
        }
    }

    // function for count strings in file
    public static int countAmountOfLine(String pathToUnsortedFile) throws IOException {
        File file = new File(pathToUnsortedFile);
        String line;
        int amountOfLine = 0;

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            while ((line = bufferedReader.readLine()) != null) {
                amountOfLine++;
            }
        } catch (IOException ex) {
            throw new IOException(ex.getMessage());
        }
        return amountOfLine;
    }

    // write content of list to a file
    public static void writeToFile(String filePath, List<String> sortedContent) {
        Path out = Paths.get(filePath);
        try {
            Files.write(out, sortedContent, Charset.defaultCharset());
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    // convert Bytes in MB
    public static double convertBytesInMB(long sizeInBytes) {
        return sizeInBytes / (double) MEGABYTE;
    }
}
