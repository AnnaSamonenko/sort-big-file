package main;

import helper.FileHelper;
import util.MergeOfFiles;

import java.io.IOException;

public class Main {
    // project path
    private static final String PROJECT_PATH = System.getProperty("user.dir");

    // directories
    private static final String DIR_FOR_UNSORTED_BIG_FILE = PROJECT_PATH + "/UnsortedBigFile/";
    private static final String DIR_FOR_FILE_PARTS = PROJECT_PATH + "/Parts/";
    private static final String DIR_FOR_SORTED_BIG_FILE = PROJECT_PATH + "/SortedBigFile/";

    // file names
    private static final String FILE_UNSORTED = "unsorted.txt";
    private static final String FILE_SORTED = "sorted.txt";

    public static void main(String[] args) {
        long startTime = System.nanoTime();

        // 1) divide big file and sort small parts of it - 69 sec for 1gb
        try {
            FileHelper.divideAndSortFile(DIR_FOR_UNSORTED_BIG_FILE + FILE_UNSORTED, DIR_FOR_FILE_PARTS, 100);
        } catch (IOException ex) {
            System.out.println("Some exception occurs during division|sort of file");
            return;
        }

        // 2) merge files - 43 sec for 1gb
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
