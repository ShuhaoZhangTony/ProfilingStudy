package flink.applications.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class zipf {
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

        int size = 99171;

        int sentenceLength = 10;

        Scanner sc = new Scanner(new File("./data/words"));
        HashMap hm = new HashMap();
        int m = 0;
        while (sc.hasNext()) {
            hm.put(m++, sc.next());
        }


//        long before = 0;
//        long after = 0;

        int n = 320000000;//1M

        double skew = 0.0;
        //ZipfGenerator z0 = new ZipfGenerator(size, skew);
        FastZipfGenerator z1 = new FastZipfGenerator(size, skew);
        PrintWriter writer = new PrintWriter("./data/books/Skew0.dat", "UTF-8");
        for (int i = 1; i <= n; i++) {
            int k = z1.next();

            //writer.print(k+" ");//for test
            writer.print(hm.get(k) + " ");

            if (i % sentenceLength == 0)
                writer.println();
            //counts.put(k, counts.get(k)+1);
        }
        writer.close();

//
//
//         writer = new PrintWriter("./data/books/Skew1.dat", "UTF-8");
//         skew = 1.0;
//         z1 = new FastZipfGenerator(size, skew);
//        for (int i=1; i<=n; i++)
//        {
//            int k = z1.next();
//
//            //writer.print(k+" ");//for test
//            writer.print(hm.get(k)+" ");
//            if(i%sentenceLength==0)
//                writer.println();
//
//        }
//        writer.close();
//
//
//
//        skew = 2.0;
//        z1 = new FastZipfGenerator(size, skew);
//         writer = new PrintWriter("./data/books/Skew2.dat", "UTF-8");
//        for (int i=1; i<=n; i++)
//        {
//            int k = z1.next();
//
//            //writer.print(k+" ");//for test
//            writer.print(hm.get(k)+" ");
//
//            if(i%sentenceLength==0)
//                writer.println();
//
//        }
//        writer.close();
//


//        before = System.nanoTime();
//        Map<Integer, Integer> counts0 = computeCounts(z0, size, n);
//        after = System.nanoTime();
//        System.out.println(counts0+", duration "+(after-before)/1e6);

//        before = System.nanoTime();
//        Map<Integer, Integer> counts1 = computeCounts(z1, size, n);
//        after = System.nanoTime();
//        System.out.println(counts1+", duration "+(after-before)/1e6);
    }

    private static Map<Integer, Integer> computeCounts(
            ZipfGenerator z, int size, int n) {
        Map<Integer, Integer> counts = new LinkedHashMap<Integer, Integer>();
        for (int i = 1; i <= size; i++) {
            counts.put(i, 0);
        }
        for (int i = 1; i <= n; i++) {
            int k = z.next();
            counts.put(k, counts.get(k) + 1);
        }
        return counts;
    }

    private static Map<Integer, Integer> computeCounts(
            FastZipfGenerator z, int size, int n) {
        Map<Integer, Integer> counts = new LinkedHashMap<Integer, Integer>();
        for (int i = 1; i <= size; i++) {
            counts.put(i, 0);
        }
        for (int i = 1; i <= n; i++) {
            int k = z.next();
            counts.put(k, counts.get(k) + 1);
        }
        return counts;
    }

}

// Based on http://diveintodata.org/tag/zipf/
class ZipfGenerator {
    private Random rnd = new Random(0);
    private int size;
    private double skew;
    private double bottom = 0;

    public ZipfGenerator(int size, double skew) {
        this.size = size;
        this.skew = skew;

        for (int i = 1; i <= size; i++) {
            this.bottom += (1 / Math.pow(i, this.skew));
        }
    }

    // the next() method returns an random rank id.
    // The frequency of returned rank ids are follows Zipf distribution.
    public int next() {
        int rank;
        double friquency = 0;
        double dice;

        rank = rnd.nextInt(size) + 1;
        friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        dice = rnd.nextDouble();

        while (!(dice < friquency)) {
            rank = rnd.nextInt(size) + 1;
            friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
        }

        return rank;
    }


    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }
}


class FastZipfGenerator {
    private Random random = new Random(0);
    private NavigableMap<Double, Integer> map;

    FastZipfGenerator(int size, double skew) {
        map = computeMap(size, skew);
    }

    private static NavigableMap<Double, Integer> computeMap(
            int size, double skew) {
        NavigableMap<Double, Integer> map =
                new TreeMap<Double, Integer>();

        double div = 0;
        for (int i = 1; i <= size; i++) {
            div += (1 / Math.pow(i, skew));
        }

        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (1.0d / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

    public int next() {
        double value = random.nextDouble();
        return map.ceilingEntry(value).getValue() + 1;
    }

}