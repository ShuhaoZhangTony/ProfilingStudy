package flink.applications.sink;

import backtype.storm.generated.NotAliveException;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import flink.applications.constants.LinearRoadFConstants;
import flink.applications.topology.special_LRF.TopologyControl;
import org.apache.flink.storm.api.FlinkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

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

    public static Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is;

        // if (classpath) {
        // is = FlinkRunner.class.getResourceAsStream(filename);
        // } else {
        // is = new FileInputStream(filename);
        // }
        is = new FileInputStream(filename);
        properties.load(is);
        is.close();

        return properties;
    }

//    private void loadConfig() {
//        String CFG_PATH = "C://Users//szhang026//Documents//costawaregrouping-flink//src//main//resources//config//%s.properties";
//        String cfg = String.format(CFG_PATH, "linear-road-full");
//        Properties p = null;
//        try {
//            p = loadProperties(cfg);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        config = flink.applications.util.config.Configuration.fromProperties(p);
//    }

    @Override
    public void initialize() {
        // loadConfig();
        super.initialize();

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
                LOG.info("Finished execution in:" + ((end - start_true) / 1000.0) / 1000000.0 + " seconds");
                Map conf = Utils.readStormConfig();
                // Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
                FlinkClient client = new FlinkClient(conf, "localhost", 6123);
                try {
                    client.killTopology("linear-road-full"); //provide topology name
                    System.exit(0);
                } catch (NotAliveException e) {
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
