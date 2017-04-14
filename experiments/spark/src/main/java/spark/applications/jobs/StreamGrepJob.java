package spark.applications.jobs;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import spark.applications.constants.StreamGrepConstants;
import spark.applications.constants.WordCountConstants;
import spark.applications.functions.Grep;
import spark.applications.spout.MemFileSpout;
import spark.applications.util.OsUtils;

import java.io.PrintWriter;


public class StreamGrepJob extends spark.applications.jobs.AbstractJob {
    private static final Logger LOG = LogManager.getLogger(StreamGrepJob.class);
    private transient PrintWriter outLogger = null;

    public StreamGrepJob(String topologyName, SparkConf config) {
        super(topologyName, config);
        batch = config.getInt("batch", 1);
        count_number = config.getInt("count_number", 1);
    }

    @Override
    public void initialize(JavaStreamingContext ssc) {
        spout = MemFileSpout.load_spout(ssc, config, getConfigPrefix(), count_number, batch);
    }

    @Override
    public JavaDStream buildJob(JavaStreamingContext ssc) {

        /*
        * we'll make use of updateStateByKey so Spark streaming will maintain a value for every key in our dataset.
        * updateStateByKey takes in a different reduce function
        * */

        // A checkpoint is configured, or else mapWithState will complaint.
        ssc.checkpoint(config.get("metric_path") + OsUtils.OS_wrapper("checkpoints")); // If spark crashes, it stores its previous state in this directory, so that it can continue when it comes back online.

        JavaDStream<String> inputTuples = ssc.queueStream(spout);

        JavaDStream<String> sentence = inputTuples.flatMap(new Grep());

        return sentence;

    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return StreamGrepConstants.PREFIX;
    }

}
