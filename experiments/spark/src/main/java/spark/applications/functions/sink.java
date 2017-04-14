package spark.applications.functions;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.applications.jobs.StreamGrepJob;
import spark.applications.util.OsUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by I309939 on 7/21/2016.
 */
public class sink implements VoidFunction {
    private static final Logger LOG = LogManager.getLogger(sink.class);
    final Accumulator<Integer> accum;
    private final int batch;

    private boolean pid, need_warm_up;
    private long start = 0, end = 0;
    private long warm_start = 0, warm_end = 0;
    private double warm_up;
    private double duration;

    public sink(JavaStreamingContext ssc, int batch, SparkConf sparkConf) {
        accum = ssc.sparkContext().accumulator(0);
        this.batch = batch;
        initialize(sparkConf);
    }

    @Override
    public void call(Object o) throws Exception {
       // final List s = ((JavaRDD) o).collect();
        execute(accum);
    }

    private void initialize(SparkConf sparkConf) {
        need_warm_up = true;
        duration = Double.parseDouble(String.valueOf(sparkConf.getInt("runtimeInSeconds", 0))) * Math.pow(10, 9);
        warm_up = Double.parseDouble(String.valueOf(sparkConf.getInt("runtimeInSeconds", 0))) * Math.pow(10, 9) / 2;
        LOG.info("test duration," + duration + "and warm_up time: " + warm_up);
        warm_start = System.nanoTime();
    }

    private void execute(Accumulator<Integer> accum) {
        accum.add(batch);//every time Spark finishes processing a whole batch..
        if (need_warm_up) {
            warm_end = System.nanoTime();
            if ((warm_end - warm_start) > warm_up)//test if warm up ended.
            {
                need_warm_up = false;
            }
        }

        if (pid && !need_warm_up) {//actual processing started.
            //sink_pid();
            pid = false;
            start = System.nanoTime();
        }

        if (!pid && !need_warm_up) {//actual processing started and not finished..
            end = System.nanoTime();
            if ((end - start) > duration) {
                // FileWriter fw = null;
                LOG.info("tuple_processed:" + accum);
                LOG.info("elaapsed_time:" + String.valueOf((end - start)));
            }
        }
    }
}
