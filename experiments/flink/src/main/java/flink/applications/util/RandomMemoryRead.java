package flink.applications.util;

import backtype.storm.utils.MutableLong;

import java.util.Random;

public class RandomMemoryRead {
    private static MutableLong[][] array;
    private static Random rand = new Random();

    private static long random_read_write() {
        // Random rand = new Random();
        long result = 0;
        //for (int i = 0; i < 10000; i++) {
        //int read_pos1 = rand.nextInt((499 - 0) + 1) + 0;
        result
                += array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get()
                + array[rand.nextInt((9999 - 0) + 1) + 0][rand.nextInt((9999 - 0) + 1) + 0].get();
        //}
        return result;
    }

    public static void main(String[] args) {
        array = new MutableLong[10000][10000];
        initialize();
        while (true) {
            @SuppressWarnings("unused")
            long temp = random_read_write();
        }
    }


    public static void initialize() {
        for (int i = 0; i < 10000; i++) {
            for (int j = 0; j < 10000; j++)
                array[i][j] = new MutableLong(i);
        }
    }
}
