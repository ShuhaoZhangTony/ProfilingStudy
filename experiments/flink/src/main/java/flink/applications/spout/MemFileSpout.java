package flink.applications.spout;

import flink.applications.constants.BaseConstants.BaseConf;
import flink.applications.spout.parser.Parser;
import flink.applications.util.OsUtils;
import flink.applications.util.config.ClassLoaderUtils;
import flink.applications.util.stream.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
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
    protected int end_index = 0;//32M
    protected int index_e = 0;
    protected int element = 0;
    transient protected BufferedWriter writer;
    long start = 0, end = 0;
    private boolean finished = false;

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
        String path = config.getString(getConfigKey(OS_prefix.concat(BaseConf.SPOUT_PATH)));

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
        Scanner scanner = new Scanner(new File(fileName),"UTF-8");

        if (config.getInt("batch") == -1) {
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
        config.put("end_index", end_index);
        LOG.info("Spout_end:" + end_index);

        RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();

        String jvmName = runtimeBean.getName();
        long pid = Long.valueOf(jvmName.split("@")[0]);
        LOG.info("JVM PID  = " + pid);

        FileWriter fw;
        try {
            if(OsUtils.isWindows()) {
                fw = new FileWriter(new File(config.getString("metrics.output") + "\\spout_threadId.txt"));
            }else{
                fw = new FileWriter(new File(config.getString("metrics.output") + "/spout_threadId.txt"));
            }
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
            fw = new FileWriter(new File(config.getString("metrics.output") + "/spout_fail.txt"));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            writer.write("Failed:" + msgId + "\n");
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

        //System.out.println("Spout index:" + index_e);
        if (index_e == array.length) {
            index_e = 0;
        }
        value = array[index_e];
        if (value != null) {
            List<StreamValues> tuples = null;
            tuples = parser.parse(value);
            // if (tuples != null) {
            for (StreamValues values : tuples) {
                //String msgId = String.format("%d", curLineIndex++);
                collector.emit(values.getStreamId(), values);
            }
            // }
        } else {
            System.out.println("Value is null!");
        }
        index_e++;
    }
}	
