package storm.applications.spout;

import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.parser.Parser;
import storm.applications.util.OsUtils;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.stream.StreamValues;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchMemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(BatchMemFileSpout.class);
    private static LinkedList<String> logger = new LinkedList<String>();
    protected Parser parser;
    protected File files;
    protected String[] array;
    protected int curFileIndex = 0;
    protected int curLineIndex = 0;
    protected int taskId;
    protected int numTasks;
    protected int start_index = 0;
    //protected int end_index=1000000;//1M
    protected int end_index = 32000000;//32M
    protected int index_e = 0;
    protected int element = 0;
    transient protected BufferedWriter writer;
    long start = 0, end = 0;
    private boolean finished = false;
    private int batch;

    @Override
    public void initialize() {
        taskId = context.getThisTaskIndex();//context.getThisTaskId(); start from 0..

        numTasks = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS));


        String OS_prefix = null;

        if (OsUtils.isWindows()) {
            OS_prefix = "win.";
        } else {
            OS_prefix = "unix.";
        }

        String path = config.getString(getConfigKey(OS_prefix.concat(BaseConf.Batch_SPOUT_PATH)));


        String parserClass = config.getString(getConfigKey(BaseConf.SPOUT_PARSER));
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        List<String> str_l = new LinkedList<String>();

        try {
            openFile(path, str_l);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void openFile(String fileName, List<String> str_l) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(fileName), "UTF-8");
        batch = config.getInt("batch");
        if (batch == -1) {
            while (scanner.hasNext()) {
                str_l.add(scanner.next());//for micro-benchmark only
            }
        } else {
            while (scanner.hasNextLine()) {
                str_l.add(scanner.nextLine()); //normal..
            }
        }
        scanner.close();
        array = str_l.toArray(new String[str_l.size()]);
        end_index = array.length * config.getInt("count_number");
        System.out.println("spout end:" + end_index);
        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        //System.out.println("JVM PID  = " + pid);


        FileWriter fw;
        try {

            fw = new FileWriter(new File(config.getString("metrics.output") + OsUtils.OS_wrapper("spout_threadId.txt")));

            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            writer.write(String.valueOf(pid));
            writer.flush();
            //writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void fail(Object msgId) {
        FileWriter fw = null;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output") + OsUtils.OS_wrapper("spout_fail.txt")));
            writer = new BufferedWriter(fw);

            writer.write("Failed:" + (String) msgId + "\n");
            writer.flush();

            fw = new FileWriter(new File(config.getString("metrics.output") + OsUtils.OS_wrapper("sink.txt")));

            writer = new BufferedWriter(fw);

            writer.write("-1");

            writer.flush();
            writer.close();

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

    @Override
    public void nextTuple() {
        String[] value = new String[batch];
        if (index_e < end_index) {
            for (int i = 0; i < batch; i++) {
                value[i] = array[index_e++ % array.length];//batched value.
            }
            if (value != null) {
                List<StreamValues> tuples = null;
                try {
                    tuples = parser.parse(value);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //tuples = parser.parse(value);
                if (tuples != null) {
                    for (StreamValues values : tuples) {
                        String msgId = String.format("%d", curLineIndex++);
                        collector.emit(values.getStreamId(), values, msgId);
                    }
                }
            }
        }
//        if(index_e==end_index){
//        	end=System.nanoTime();
//        }
    }
}	
