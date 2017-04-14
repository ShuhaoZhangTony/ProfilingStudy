package spark.applications.functions.original;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.applications.constants.BaseConstants;
import spark.applications.util.OsUtils;
import spark.applications.util.propertiesUtil;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;

/**
 * Created by szhang026 on 5/4/2016.
 * new Function<JavaPairRDD<String, Integer>, JavaRDD<Integer>>() {
 *
 * @Override public JavaRDD<Integer> call(JavaPairRDD<String, Integer> v1) throws Exception {
 * return v1.values();
 * }
 * }
 */
public class NullSink implements Function<JavaPairRDD<String, Integer>, Void>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(NullSink.class);
    private transient PrintWriter outLogger = null;
    private int end_index = 0;
    private long start_true;
    private long end;
    JavaStreamingContext ssc;
    private Long[] global_counter;

    public NullSink(JavaStreamingContext ssc, Long[] global_counter, String prefix) {
        this.ssc = ssc;
        this.global_counter = global_counter;
        SparkConf sparkConf = propertiesUtil.load("word-count");
        end_index = sparkConf.getInt("end_index", 0) * sparkConf.getInt("count_number", 1);
        LOG.info("end_index:" + end_index);
        try {

            String OS_prefix = null;
            String path;
            if (OsUtils.isWindows()) {
                OS_prefix = "win.";
            } else {
                OS_prefix = "unix.";
            }
            outLogger = new PrintWriter(new File(sparkConf.get(String.format(OS_prefix.concat(BaseConstants.BaseConf.SINK_PATH), prefix))));

        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    @Override
    public Void call(JavaPairRDD<String, Integer> v1) throws Exception {
        global_counter[0]++;
        if (global_counter[0] == 1) {
            start_true = System.nanoTime();
        }
        if (global_counter[0] == end_index) {
            end = System.nanoTime();
            outLogger.print(String.valueOf((end - start_true) / 1000000000));
            outLogger.flush();
            outLogger.close();
            ssc.stop(true, true);
        }
        return null;
    }
}
