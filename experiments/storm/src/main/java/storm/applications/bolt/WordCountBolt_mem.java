package storm.applications.bolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.WordCountConstants.Field;

import java.io.BufferedWriter;
import java.util.Random;

public class WordCountBolt_mem extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt_mem.class);
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
    private MutableLong[][] array = new MutableLong[10000][10000];// long: 32 bits L3
    // (per socket):20M
    // * 8 bits --> 5M
    // long can fully
    // occupy L3.
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
        for (int i = 0; i < 10000; i++) {
            //int read_pos1 = rand.nextInt((499 - 0) + 1) + 0;
            int read_pos2 = rand.nextInt((9999 - 0) + 1) + 0;
            int write_pos = rand.nextInt((9 - 0) + 1) + 0;
            array_read[write_pos] = array[i][read_pos2].get();
        }
        return array_read;
    }

    @Override
    public void initialize() {
        for (int i = 0; i < 10000; i++) {
            for (int j = 0; j < 10000; j++)
                array[i][j] = new MutableLong(i);
        }
    }

    @Override
    public void execute(Tuple input) {
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
}
