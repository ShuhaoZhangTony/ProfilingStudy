package flink.applications.sink;

import backtype.storm.generated.NotAliveException;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import org.apache.flink.storm.api.FlinkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author mayconbordin
 */
public class NullSink_FD extends BaseSink {
    private static Logger LOG;
    protected int index_e, end_index;
    //   protected ArrayList<String> recorder = new ArrayList<String>();
    transient protected BufferedWriter writer;
    long start_true, end;

    //private final int tenM=10000000;
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

    @Override
    public void initialize() {
        LOG = LoggerFactory.getLogger(NullSink_FD.class);
        start_true = 0;
        end = 0;
        end_index = 0;
        //loadConfig();
    }

    @Override
    public void execute(Tuple input) {
        // do nothing
        collector.ack(input);
//        System.out.println(index_e);
        index_e++;
        if (!input.getValues().isEmpty()) {
            if (index_e == 1) {

                end_index = config.getInt("end_index");
                LOG.info("end_index:" + String.valueOf(end_index));
                start_true = System.nanoTime();
            }
            if (index_e == end_index) {//320M
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
                    System.out.println("Finished execution:" + (end - start_true) / 1000000000);
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
                FlinkClient client = new FlinkClient(conf, "localhost", 6123);
                try {

                    client.killTopology("fraud-detection"); //provide topology name
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
