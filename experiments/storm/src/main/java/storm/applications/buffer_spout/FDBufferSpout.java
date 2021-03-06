package storm.applications.buffer_spout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.AbstractSpout;
import storm.applications.spout.parser.Parser;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.stream.StreamValues;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class FDBufferSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(FDBufferSpout.class);
    private static LinkedList<String> logger = new LinkedList<String>();
    protected Parser parser;
    protected File files;
    protected String[] array;
    protected int curFileIndex = 0;
    protected int curLineIndex = 0;
    protected int taskId;
    protected int numTasks;
    protected int start_index = 0;
    protected int end_index = 32000000;//32M
    protected int index_e = 0;
    protected int element = 0;
    transient protected BufferedWriter writer;
    long start = 0, end = 0;
    private boolean finished = false;

    @Override
    public void initialize() {
        taskId = context.getThisTaskIndex();//context.getThisTaskId(); start from 0..

        numTasks = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS));
        String path = config.getString(getConfigKey(BaseConf.SPOUT_PATH));
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
        Scanner scanner = new Scanner(new File(fileName));
        while (scanner.hasNextLine()) {
            str_l.add(scanner.nextLine());
        }
        scanner.close();
        array = str_l.toArray(new String[str_l.size()]);
        end_index = array.length * config.getInt("count_number");

        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output") + "\\spout_threadId.txt"));
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
        FileWriter fw;
        try {
            fw = new FileWriter(new File(config.getString("metrics.output") + "\\spout_fail.txt"));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            writer.write("Failed:" + (String) msgId + "\n");
            writer.flush();
            //writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        String value = array[Integer.parseInt((String) msgId) % array.length];
        List<StreamValues> tuples = parser.parse(value);

        if (tuples != null) {
            for (StreamValues values : tuples) {
                collector.emit(values.getStreamId(), values, msgId);
            }
        }
    }

    @Override
    public void nextTuple() {
        String value = null;
        if (index_e < end_index) {
            value = array[index_e % array.length];
            if (value != null) {
                List<StreamValues> tuples = null;
                tuples = parser.parse(value);
                if (tuples != null) {
                    for (StreamValues values : tuples) {
                        String msgId = String.format("%d", curLineIndex++);
                        collector.emit(values.getStreamId(), values, msgId);
                    }
                }
            }
            index_e++;
        }
//        if(index_e==end_index){
//        	end=System.nanoTime();
//        }        
    }
}	
