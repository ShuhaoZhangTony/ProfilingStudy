package spark.applications.spout;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.applications.constants.BaseConstants;
import spark.applications.util.OsUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

/**
 * Created by szhang026 on 5/6/2016.
 */
public class MemFileSpout {

    /**
     * Utility method to parse a configuration key with the application prefix and
     * component prefix.
     *
     * @param key  The configuration key to be parsed
     * @param name The name of the component
     * @return The formatted configuration key
     */
    protected static String getConfigKey(String key, String name, String prefix) {
        return String.format(key, String.format("%s.%s", prefix, name));
    }

    /**
     * Utility method to parse a configuration key with the application prefix..
     *
     * @param key The configuration key to be parsed
     * @return
     */
    protected static String getConfigKey(String key, String prefix) {
        return String.format(key, prefix);
    }

    public static Queue<JavaRDD<String>> load_spout(JavaStreamingContext ssc, SparkConf config, String prefix, int count_number, int batch) {
        // Create the queue through which RDDs can be pushed to
        // a QueueInputDStream
        Queue<JavaRDD<String>> spout = new LinkedList<JavaRDD<String>>();
        String OS_prefix = null;

        String path;
        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }

        path = config.get(getConfigKey(OS_prefix.concat(BaseConstants.BaseConf.SPOUT_PATH), prefix));

        for (int i = 0; i < count_number; i++) {
            Scanner scanner = null;
            try {
                scanner = new Scanner(new File(path), "UTF-8");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            int j = 0;
            List<String> str_l = new LinkedList<String>();
            while (scanner.hasNextLine()) {
                if (j < batch) {
                    str_l.add(scanner.nextLine()); //normal..
                    j++;
                } else {
                    spout.add(ssc.sparkContext().parallelize(str_l));
                    j = 0;
                    str_l = new LinkedList<String>();//create the list for this batch.
                }
            }
            scanner.close();
        }
        return spout;
    }
}
