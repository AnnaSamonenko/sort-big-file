package main;

import merge.MergeHelper;
import split.SplitHelper;

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

//        try (SplitHelper splitHelper = new SplitHelper(DIR_FOR_UNSORTED_BIG_FILE + FILE_UNSORTED)) {
//            (splitHelper.runDivideAndSortInParallel(DIR_FOR_FILE_PARTS)).await();
//        } catch (IOException ex) {
//            System.out.println("Some exception occurs during division of file");
//            return;
//        }

        try (MergeHelper mergeHelper = new MergeHelper(DIR_FOR_SORTED_BIG_FILE, FILE_SORTED)) {
            (mergeHelper.mergeInParallel(DIR_FOR_FILE_PARTS)).await();
        } catch (IOException e) {
            System.out.println("Some exception occurs during merge of files");
            return;
        }

        long endTime = System.nanoTime();
        System.out.print((endTime - startTime) / 1000000000);
    }
}
