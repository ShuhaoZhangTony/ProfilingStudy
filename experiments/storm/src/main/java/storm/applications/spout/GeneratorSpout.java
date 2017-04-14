package storm.applications.spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.generator.Generator;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.stream.StreamValues;

import java.io.BufferedWriter;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
    transient protected BufferedWriter writer;
    StreamValues values = null;
    private Generator generator;
    private long count = 0;

    @Override
    protected void initialize() {
        String generatorClass = config.getString(getConfigKey(BaseConf.SPOUT_GENERATOR));
        generator = (Generator) ClassLoaderUtils.newInstance(generatorClass, "parser", LOG);
        generator.initialize(config);

        //Configuration Conf = Configuration.fromMap(config);
        //count=Conf.getInt("count", 1000);

    }

    @Override
    public void nextTuple() {
        if (count < 10000000)
            values = generator.generate();
        else
            values = null;
//    	if(count==0){
//    		FileWriter fw;
//        	try {
//    			fw = new FileWriter(new File(config.getString("metrics.output")+"\\CDR.txt"));
//    			writer = new BufferedWriter(fw);
//    		} catch (IOException e1) {
//    			// TODO Auto-generated catch block
//    			e1.printStackTrace();
//    		}
//    	}

//        if (values == null) {
//        	try {
//				writer.close();
//			} catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//        	System.out.println("Finished, try to kill");
//            Map conf = Utils.readStormConfig();
//            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
//            try {
//                List<TopologySummary> topologyList = client.getClusterInfo().get_topologies();
//              while(topologyList.size()==0)
//                  topologyList = client.getClusterInfo().get_topologies();
//
//               // KillOptions killOpts = new KillOptions();
//                    //killOpts.set_wait_secs(waitSeconds) // time to wait before killing
//                client.killTopology(topologyList.get(0).get_name()); //provide topology name
//
//            } catch (TException e) {
//                e.printStackTrace();
//            } catch (NotAliveException e) {
//                e.printStackTrace();
//            }
//        }
        if (values != null) {
//        	try {
//    			writer.write(String.valueOf(values)+"\n");
//    			writer.flush();
//    			//writer.close();
//    		} catch (IOException e) {
//    			// TODO Auto-generated catch block
//    			e.printStackTrace();
//    		}
            collector.emit(values.getStreamId(), values);
        }
        count++;
    }
}
