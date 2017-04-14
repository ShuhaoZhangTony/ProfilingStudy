package spark.applications.jobs;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import java.util.Queue;

public abstract class AbstractJob {
    protected Queue spout;
    protected String topologyName;
    protected SparkConf config;
    protected int batch;
    protected int count_number = 0;

    public AbstractJob(String topologyName, SparkConf config) {
        this.topologyName = topologyName;
        this.config = config;
    }


    public abstract void initialize(JavaStreamingContext ssc);

    //build job to get the last JavaDStream.
    //with input as the first JavaDStream
    public abstract JavaDStream<String> buildJob(JavaStreamingContext ssc);

    public abstract Logger getLogger();

    public abstract String getConfigPrefix();
}