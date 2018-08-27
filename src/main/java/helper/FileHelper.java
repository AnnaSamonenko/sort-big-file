package helper;

import util.StringGenerator;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

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

    // convert Bytes in MB
    public static double convertBytesInMB(long sizeInBytes) {
        return sizeInBytes / (double) MEGABYTE;
    }
}
