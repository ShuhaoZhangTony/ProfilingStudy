package storm.applications.sink;

import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.util.OsUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

    public void initialize() {
        super.initialize();
        index_e = 0;
        pid = true;
        need_warm_up = true;
        duration = Double.parseDouble(String.valueOf(config.getInt("runtimeInSeconds"))) * Math.pow(10, 9);
        warm_up = Double.parseDouble(String.valueOf(config.getInt("runtimeInSeconds"))) * Math.pow(10, 9) / 2;
        LOG.info("test duration," + duration + "and warm_up time: " + warm_up);
        end_index = config.getInt("end_index");
        System.out.println(end_index);
        warm_start = System.nanoTime();
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
        if (need_warm_up) {
            warm_end = System.nanoTime();
            if ((warm_end - warm_start) > warm_up)//test if warm up ended.
            {
                need_warm_up=false;
            }
        }

        if (pid && !need_warm_up) {//actual processing started.
            sink_pid();
            pid = false;
            start = System.nanoTime();
        }

        if (!pid && !need_warm_up) {//actual processing started and not finished..
            end = System.nanoTime();
            if ((end - start) > duration) {
                FileWriter fw = null;
                try {
                    fw = new FileWriter(new File(config.getString("metrics.output") + OsUtils.OS_wrapper("tuple_processed.txt")));
                    writer = new BufferedWriter(fw);
                    writer.write(String.valueOf(index_e));
                    writer.flush();
                    writer.close();


                    fw = new FileWriter(new File(config.getString("metrics.output") + OsUtils.OS_wrapper("elapsed_time.txt")));
                    writer = new BufferedWriter(fw);
                    writer.write(String.valueOf((end - start)));
                    writer.flush();
                    writer.close();

                    //LOG.info("Finished execution in:"+((end-start)/1000.0)/1000000.0+" seconds");
                    Map conf = Utils.readStormConfig();
                    Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
                    try {
                        List<TopologySummary> topologyList = client.getClusterInfo().get_topologies();
                        while (topologyList.size() == 0)
                            topologyList = client.getClusterInfo().get_topologies();

                        KillOptions killOpts = new KillOptions();
                        killOpts.set_wait_secs(10); // time to wait before killing
                        while (topologyList.size() != 0) {
                            client.killTopologyWithOpts(topologyList.get(0).get_name(), killOpts); //provide topology name
                            TimeUnit.SECONDS.sleep(1);
                        }
                    } catch (AuthorizationException e) {
                        e.printStackTrace();
                    } catch (NotAliveException e) {
                        e.printStackTrace();
                    } catch (TException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
