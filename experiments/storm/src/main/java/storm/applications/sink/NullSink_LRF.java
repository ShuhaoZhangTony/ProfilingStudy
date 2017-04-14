package storm.applications.sink;

import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.topology.special_LRF.TopologyControl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author mayconbordin
 */
public class NullSink_LRF extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(NullSink_LRF.class);
    protected int index_e1, end_index1 = 100;
    protected int index_e2, end_index2 = 100;
    protected ArrayList<String> recorder = new ArrayList<String>();
    transient protected BufferedWriter writer;
    long start = System.nanoTime(), start_true = 0, end = 0;
    boolean read1 = true, read2 = true, read3 = true, read4 = true;
    private int index_e3, end_index3 = 100;
    private int index_e4, end_index4 = 100;

    //private final int tenM=10000000;

    public void initialize() {
        super.initialize();
        index_e1 = index_e2 = index_e3 = index_e4 = 0;
        start_true = end = 0;
        read1 = read2 = read3 = read4 = true;
    }

    @Override
    public void execute(Tuple input) {
        // do nothing
        collector.ack(input);
//        System.out.println(index_e);

        switch (input.getSourceStreamId()) {
            case TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID:
                index_e1++;
                if (index_e1 == 1 && read1) {
                    end_index1 = config.getInt("end_index1");
                    LOG.info("end_index1:" + String.valueOf(end_index1));
                    start_true = System.nanoTime();
                    read1 = false;
                }
                break;
            case TopologyControl.ACCIDENTS_NOIT_STREAM_ID:
                index_e2++;
                if (index_e2 == 1 && read2) {
                    end_index2 = config.getInt("end_index2");
                    LOG.info("end_index2:" + String.valueOf(end_index2));
                    read2 = false;
                }
                break;
            case TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID:
                index_e3++;
                if (index_e3 == 1 && read3) {
                    end_index3 = config.getInt("end_index3");
                    LOG.info("end_index3:" + String.valueOf(end_index3));
                    read3 = false;
                }
                break;
            default:
                index_e4++;
                if (index_e4 == 1 && read4) {
                    end_index4 = config.getInt("end_index4");
                    LOG.info("end_index4:" + String.valueOf(end_index4));
                    read4 = false;
                }
                break;
        }

        if (!input.getValues().isEmpty()) {
//            if (index_e1 > 10000)
//                LOG.info("index_e1:" + index_e1);
//            LOG.info("index_e2:" + index_e2);
//            LOG.info("index_e3:" + index_e3);
//            LOG.info("index_e4:" + index_e4);

            if (index_e1 >= end_index1 && index_e2 >= end_index2 && index_e3 >= end_index3 && index_e4 >= end_index4) {//320M
                end = System.nanoTime();
                try {
                    FileWriter fw;
                    try {
                        fw = new FileWriter(new File(config.getString("metrics.output") + "/sink.txt"));
                        writer = new BufferedWriter(fw);
                        //writer.write("Received first element\n");
                        //writer.flush();

                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
//        		//System.out.println("Finished execution in:"+((end-start)/1000.0)/1000000.0+" seconds"+"index_e"+index_e);
                    //writer.write(start_true+"\n");

//    			for(int i=0;i<recorder.size();i++){
//    				writer.write(recorder.get(i)+"\n");
//    			}
                    writer.write(String.valueOf((end - start_true) / 1000000000));
                    //writer.write(((end-start)/1000.0)/1000000.0+"\t"+index_e+"\n");
                    //writer.write(((end-start_true)/1000.0)/1000000.0+"\t"+index_e+"\n");
                    writer.flush();
                    writer.close();
//                try {
//					TimeUnit.SECONDS.sleep(5);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
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
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
