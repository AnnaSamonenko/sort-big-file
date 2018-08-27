package main;

import helper.FileHelperInParallel;
import util.MergeOfFiles;

import java.io.IOException;

public class Main {

    private static final String PROJECT_PATH = System.getProperty("user.dir");

    private static final String DIR_FOR_UNSORTED_BIG_FILE = PROJECT_PATH + "/UnsortedBigFile/";
    private static final String DIR_FOR_FILE_PARTS = PROJECT_PATH + "/Parts/";
    private static final String DIR_FOR_SORTED_BIG_FILE = PROJECT_PATH + "/SortedBigFile/";

    private static final String FILE_UNSORTED = "unsorted.txt";
    private static final String FILE_SORTED = "sorted.txt";

    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();

        try (FileHelperInParallel fileHelperIP = new FileHelperInParallel(DIR_FOR_UNSORTED_BIG_FILE + FILE_UNSORTED)) {
            (fileHelperIP.runDivideAndSortInParallel(DIR_FOR_FILE_PARTS)).await();
        } catch (IOException ex) {
            System.out.println("Some exception occurs during merge of files");
            return;
        }

        try (MergeOfFiles mergeOfFiles = new MergeOfFiles(DIR_FOR_SORTED_BIG_FILE, FILE_SORTED)) {
            mergeOfFiles.merge(DIR_FOR_FILE_PARTS);
        } catch (IOException e) {
            System.out.println("Some exception occurs during merge of files");
            return;
        }

        long endTime = System.nanoTime();
        System.out.print((endTime - startTime) / 1000000000);
    }
}
