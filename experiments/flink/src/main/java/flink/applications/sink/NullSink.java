package flink.applications.sink;

import backtype.storm.tuple.Tuple;
import flink.applications.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;

/**
 * @author mayconbordin
 */
public class NullSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(NullSink.class);
    protected int index_e, end_index = 0;
    protected ArrayList<String> recorder = new ArrayList<String>();
    transient protected BufferedWriter writer;
    long start = 0, end = 0;
    long warm_start = 0, warm_end = 0;
    private boolean pid;
    double duration = 150000000000.0;
    private double warm_up;
    private boolean need_warm_up;
    private int checkPoint;
    private double measure_interval;
    private ArrayList<String> strings = new ArrayList<>();
    private double measure_times;

    public void initialize() {
        super.initialize();
        index_e = 0;
        pid = true;
        need_warm_up = false;//we test warm up directly! We need to bypass warm up stage only when we need to profile!
        duration = Double.parseDouble(String.valueOf(config.getInt("runtimeInSeconds"))) * Math.pow(10, 9);
        warm_up = Double.parseDouble(String.valueOf(config.getInt("runtimeInSeconds"))) * Math.pow(10, 9) / 2;

        //interval at per 100 ms.
        measure_interval = Double.parseDouble(String.valueOf(100)) * Math.pow(10, 6);
        measure_times=duration/measure_interval;
        LOG.info("test duration," + duration + "and warm_up time: " + warm_up);
        end_index = config.getInt("end_index");
        System.out.println(end_index);
        warm_start = System.nanoTime();
        checkPoint = 0;
    }

    private void sink_pid() {
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        LOG.info("JVM PID  = " + pid);

        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output") + OsUtils.OS_wrapper("sink_threadId.txt")));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            String s_pid = String.valueOf(pid);
            writer.write(s_pid);
            writer.flush();
            //writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        // do nothing
        collector.ack(input);
        index_e++;
//        if (need_warm_up) {
//            warm_end = System.nanoTime();
//            if ((warm_end - warm_start) > warm_up)//test if warm up ended.
//            {
//                need_warm_up = false;
//            }
//        }

        if (pid && !need_warm_up) {//actual processing started.
            sink_pid();
            pid = false;
            start = System.nanoTime();
        }

        if (!pid && !need_warm_up) {//actual processing started and not finished..
            end = System.nanoTime();
            if ((end - start) > measure_interval) {
                checkPoint++;
                strings.add("Throughput:" + String.valueOf((index_e * 1000000.0 / (end - start))));
                index_e=0;//clear index_e
                if (checkPoint == measure_times) {
                    FileWriter fw = null;
                    try {
                        fw = new FileWriter(new File(config.getString("metrics.output") + OsUtils.OS_wrapper("throughput.txt")));
                        writer = new BufferedWriter(fw);
                        for (String s : strings) {
                            writer.write(s.concat("\n"));
                        }
                        writer.flush();
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.exit(0);
                }
                start = System.nanoTime();
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
