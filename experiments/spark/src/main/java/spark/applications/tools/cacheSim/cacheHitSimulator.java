package spark.applications.tools.cacheSim;

import com.vividsolutions.jts.math.Vector2D;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by szhang026 on 4/23/2016.
 */
public class cacheHitSimulator {
    static String path = "C:\\Users\\szhang026\\Documents\\Profile-experiments\\4-wayMachine\\flink\\instruction_trace";

    private static double nextTime(double rateParameter) {
        return -Math.log(1.0 - Math.random()) / rateParameter;
    }


    private static int getsum(Collection<String> l, Map<String, Integer> instruction_per_function) {
        int sum = 0;
        for (String i : l) {
            sum += instruction_per_function.get(i);
        }
        return sum;
    }

    private static Vector2D cacheHit(LinkedList<String> event_trace, Map<String, Integer> instruction_per_function, int policy) throws IOException {
        final int cache_size = 32000;
        int cache_used = 0;
        int compulsory_miss = 0;
        int access_miss = 0;
        // LinkedList<Integer> cached = new LinkedList();
        Map<String, Integer> cached = new HashMap<>();//<function name, age>
        for (String i : event_trace) {
            if (policy == 1) {
                //update age
                for (String key : cached.keySet()) {
                    cached.put(key, cached.get(key) + 1);
                }
            }
            assert cache_used == getsum(cached.keySet(), instruction_per_function);
            if (!cached.containsKey(i)) {
                if (cache_used + instruction_per_function.get(i) > cache_size) {

                    switch (policy) {
                        case 0: {
                            while (cache_used + instruction_per_function.get(i) > cache_size) {
                                assert (cache_used == getsum(cached.keySet(), instruction_per_function));
                                Random random = new Random();
                                List<String> keys = new ArrayList(cached.keySet());
                                String randomKey = null;
                                try {
                                    randomKey = keys.get(random.nextInt(keys.size()));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                                cached.remove(randomKey);
                                cache_used -= instruction_per_function.get(randomKey);
                            }
                            break;
                        }
                        case 1: {
                            while (cache_used + instruction_per_function.get(i) > cache_size) {
                                int lru = 0;
                                String idx = null;
                                for (String key : cached.keySet()) {
                                    if (cached.get(key) > lru) {
                                        lru = cached.get(key);
                                        idx = key;
                                    }
                                }
                                cached.remove(idx);
                                cache_used -= instruction_per_function.get(idx);
                            }
                            break;
                        }
                    }

                    cache_used += instruction_per_function.get(i);
                    cached.put(i, 0);
                    access_miss++;
                } else {
                    cache_used += instruction_per_function.get(i);
                    cached.put(i, 0);
                    compulsory_miss++;
                }
            } else {
                cached.put(i, 0);//refresh the age to 0.
            }
        }
        return new Vector2D(compulsory_miss, access_miss);
    }

    private static LinkedList<String> clean_InTrace_results(int app) throws IOException {
        LinkedList<String> event_trace = new LinkedList<>();
        FileWriter writer = null;

        Scanner sc = null;
        switch (app) {
            case 0: {//analysis wc

                sc = new Scanner(new File(path + "\\wc.trace"));
                writer = new FileWriter(path + "\\wc.lst", false);
                break;
            }
            case 1: {//analysis fd

                sc = new Scanner(new File(path + "\\fd.trace"));
                writer = new FileWriter(path + "\\fd.lst", false);
                break;
            }
            case 2: {//analysis lg

                sc = new Scanner(new File(path + "\\lg.trace"));
                writer = new FileWriter(path + "\\lg.lst", false);
                break;
            }
            case 3: {//analysis sd

                sc = new Scanner(new File(path + "\\sd.trace"));
                writer = new FileWriter(path + "\\sd.lst", false);
                break;
            }
            case 4: {//analysis vs

                sc = new Scanner(new File(path + "\\vs.trace"));
                writer = new FileWriter(path + "\\vs.lst", false);
                break;
            }
            case 5: {//analysis tm

                sc = new Scanner(new File(path + "\\tm.trace"));
                writer = new FileWriter(path + "\\tm.lst", false);
                break;
            }
            case 6: {//analysis lr

                sc = new Scanner(new File(path + "\\lr.trace"));
                writer = new FileWriter(path + "\\lr.lst", false);
                break;
            }
        }

        String r;
        String[] r_a;
        String function;
        while (sc.hasNext()) {
            r = sc.nextLine();
            if (r.contains("{")) {
                function = r.split("\\{")[0];
                r_a = function.split("]");
                function = r_a[r_a.length - 1];
                function = function.substring(1, function.length() - 2);
                function = function.replaceAll(":", "::");
                function = function.replaceAll("\\.", "::");

                event_trace.add(function);
                writer.write(function);
                writer.write("\n");
            }
        }
        writer.flush();
        writer.close();
        return event_trace;
    }

    public static void main(String[] arg) throws IOException {

        int policy = 1;//0:random, 1:LRU...

        for (int app = 0; app < 6; app++) {

            LinkedList<String> event_trace = clean_InTrace_results(app);
            System.out.println(event_trace);
            Map<String, Integer> instruction_per_function = new HashMap<>();

            for (int i = 0; i < event_trace.size(); i++) {
                instruction_per_function.put(event_trace.get(i), CaculateInst.calculate(path + "\\" + app + "\\" + event_trace.get(i) + ".csv"));
            }

            Vector2D result = cacheHit(event_trace, instruction_per_function, policy);
            System.out.println("compulsory_miss ratio:" + result.getX() / event_trace.size());
            System.out.println("Access_miss ratio:" + result.getY() / event_trace.size());
            System.out.println("Cache Miss:" + result.getX() + result.getY() / event_trace.size());
        }
    }
}
