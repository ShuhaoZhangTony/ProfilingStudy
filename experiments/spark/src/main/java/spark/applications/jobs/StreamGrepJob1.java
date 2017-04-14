package spark.applications.jobs;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.applications.constants.WordCountConstants;
import spark.applications.functions.Grep;
import spark.applications.spout.MemFileSpout;
import spark.applications.util.OsUtils;

import java.io.PrintWriter;


public class StreamGrepJob1 extends AbstractJob {
    private static final Logger LOG = LogManager.getLogger(StreamGrepJob1.class);
    private transient PrintWriter outLogger = null;

    public StreamGrepJob1(String topologyName, SparkConf config) {
        super(topologyName, config);
        batch = config.getInt("batch", 1);
        count_number = config.getInt("count_number", 1);
    }

    @Override
    public void initialize(JavaStreamingContext ssc) {
        spout = MemFileSpout.load_spout(ssc, config, getConfigPrefix(), count_number, batch);
    }

    /**
     * This is in Driver program.
     *
     * @param ssc
     * @return
     */
    @Override
    public JavaDStream buildJob(JavaStreamingContext ssc) {

        /*
        * we'll make use of updateStateByKey so Spark streaming will maintain a value for every key in our dataset.
        * updateStateByKey takes in a different reduce function
        * */

        // A checkpoint is configured, or else mapWithState will complain.
        ssc.checkpoint(config.get("metric_path") + OsUtils.OS_wrapper("checkpoints")); // If spark crashes, it stores its previous state in this directory, so that it can continue when it comes back online.
        final Accumulator<Integer> accumulator = ssc.sparkContext().accumulator(0);
        JavaDStream<String> inputTuples = ssc.queueStream(spout);

        JavaDStream<String> sentence = inputTuples.flatMap(new Grep());
//        final Long[] global_counter = {0l};
//        sentence.map(new NullSink(ssc,global_counter,getConfigPrefix()));
        /**
         *         final Long[] global_counter = {0l};

         below is the effective sink function.
         final Long[] global_counter = {0l};
         final long[] start_true = {0l};
         final long[] end = {0l};
         final long[] end_index = {0l};
         final long[] update_size = {batch};

         SparkConf sparkConf = propertiesUtil.load("streamgrep");
         end_index[0] = sparkConf.getInt("end_index", 0) * sparkConf.getInt("count_number", 1);
         LOG.error("end_index:" + end_index[0]);

         sentence.foreachRDD(v1 -> {
         global_counter[0] += update_size[0];
         LOG.warn(global_counter[0].toString());
         if (global_counter[0] == 1) {
         start_true[0] = System.nanoTime();
         }
         if (global_counter[0] == end_index[0]) {
         end[0] = System.nanoTime();
         LOG.info("Finished Execution in:" + String.valueOf((end[0] - start_true[0]) / 1000000000));

         }
         return null;
         });
         */
        return sentence;

    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return WordCountConstants.PREFIX;
    }

}
