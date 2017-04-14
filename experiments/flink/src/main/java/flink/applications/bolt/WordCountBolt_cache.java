package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.MutableLong;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.WordCountConstants.Field;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.util.Random;

public class WordCountBolt_cache extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt_cache.class);
    private static final String splitregex = " ";
    transient protected BufferedWriter writer;
    // public WordCountBolt_mem(){
    // for(int i=0;i<200000;i++){
    // array[i]= new MutableLong(i);
    // }
    // }
    // private final Map<String, MutableLong> counts = new HashMap<>();
    // AtomicInteger index_e= new AtomicInteger();
    // long start=0,end=0,curr=0;
    // private static LinkedList<String> logger=new LinkedList<String>();
    // transient private BufferedWriter writer ;
    int curr = 0;
    private MutableLong[][] array = new MutableLong[10][10000];// long: 32 bits L3
    // (per socket):20M
    // * 8 bits --> 0.1M
    // long is significantly less than L3.
    private Random rand = new Random();
    private int total_thread = 0;

    @Override
    public Fields getDefaultFields() {
        LOG.info("WordCountBolt_mem");
        return new Fields(Field.WORD, Field.COUNT);
    }

    private long[] random_read_write() {
        // Random rand = new Random();
        long[] array_read = new long[10];
        for (int i = 0; i < 2000; i++) {
            int read_pos1 = rand.nextInt((9 - 0) + 1) + 0;
            int read_pos2 = rand.nextInt((9999 - 0) + 1) + 0;
            int write_pos = rand.nextInt((9 - 0) + 1) + 0;
            array_read[write_pos] = array[read_pos1][read_pos2].get();
        }
        return array_read;
    }

    @Override
    public void execute(Tuple input) {
        // if(curr==0){
        // RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
        //
        // String jvmName = runtimeBean.getName();
        // long pid = Long.valueOf(jvmName.split("@")[0]);
        // //System.out.println("JVM PID = " + pid);
        //
        //
        // FileWriter fw;
        // try {
        // fw = new FileWriter(new
        // File(config.getString("metrics.output")+"\\count_threadId"+Thread.currentThread().getId()+".txt"));
        // writer = new BufferedWriter(fw);
        // } catch (IOException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // try {
        // writer.write(String.valueOf(pid));
        // writer.flush();
        // writer.close();
        // } catch (IOException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // }
        curr++;
        if (curr <= 10) {
            String word = input.getStringByField(Field.WORD);
            for (int i = 0; i < 10000; i++) {
                array[curr - 1][i] = new MutableLong(i);
            }
            collector.emit(input, new Values(word, 0));
            collector.ack(input);
        } else {
            if (config.getInt("batch") == -1) {
                String word = input.getStringByField(Field.WORD);
                // MutableLong count = counts.get(word);
                //
                // if (count == null) {
                // count = new MutableLong(0);
                // counts.put(word, count);
                // }
                // count.increment();
                long rt = random_read_write()[0];
                collector.emit(input, new Values(word, rt));
                collector.ack(input);
            } else {
                String[] words = input.getString(0).split(splitregex);
                for (String word : words) {
                    if (!StringUtils.isBlank(word)) {
                        // // MutableLong count = counts.get(word);
                        //
                        // if (count == null) {
                        // count = new MutableLong(0);
                        // counts.put(word, count);
                        // }
                        // count.increment();
                        long rt = random_read_write()[0];
                        collector.emit(input, new Values(word, rt));
                    }
                }
                collector.ack(input);
            }
        }
        // if(curr==1){
        // File theDir = new File(config.getString("metrics.output")+"\\count");
        //
        // // if the directory does not exist, create it
        // if (!theDir.exists()) {
        // //System.out.println("creating directory: " + theDir);
        // boolean result = false;
        //
        // try{
        // theDir.mkdir();
        // result = true;
        // }
        // catch(SecurityException se){
        // //handle it
        // }
        //// if(result) {
        //// System.out.println("DIR created");
        //// }
        // }
        // start=System.nanoTime();
        // FileWriter fw;
        // try {
        // fw = new FileWriter(new
        // File(config.getString("metrics.output")+"\\count\\"+context.getThisTaskId()+"count.txt"));
        // writer = new BufferedWriter(fw);
        // } catch (IOException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // total_thread=config.getInt(Conf.SPLITTER_THREADS);
        // }
        // if(curr%(10000/total_thread)==0){
        // end=System.nanoTime();
        // //logger.add(((end-start)/1000.0)/1000000.0+"\t"+curr+"\n");
        // try {
        // writer.write(((end-start)/1000.0)/1000000.0+"\t"+curr+"\n");
        // writer.flush();
        //
        // } catch (IOException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // }
        // if(curr==10000000){//last element, 10M
        // //end=System.nanoTime();
        //// try {
        //// TimeUnit.SECONDS.sleep(5);
        //// } catch (InterruptedException e) {
        //// // TODO Auto-generated catch block
        //// e.printStackTrace();
        //// }
        //
        // try {
        // int length=logger.size();
        // for(int i=0;i<length;i++){
        // writer.write(logger.poll());
        // }
        // writer.flush();
        // writer.close();
        //
        // } catch (IOException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        //
        //// try {
        //// TimeUnit.SECONDS.sleep(3000);
        //// } catch (InterruptedException e) {
        //// // TODO Auto-generated catch block
        //// e.printStackTrace();
        //// }
        // }
    }

}
