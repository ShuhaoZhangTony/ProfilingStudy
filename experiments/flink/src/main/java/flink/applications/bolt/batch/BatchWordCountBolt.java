package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.MutableLong;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class BatchWordCountBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchWordCountBolt.class);
    //private int total_thread=context.getThisTaskId();
    private static final String splitregex = " ";
    private static LinkedList<String> logger = new LinkedList<String>();
    private final Map<String, MutableLong> counts = new HashMap<>();
    long start = 0, end = 0, curr = 0;
    private boolean print = false;
    transient private BufferedWriter writer;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) {
        //int batch = config.getInt("batch");
        LinkedList<String> input_l = (LinkedList<String>) input.getValue(0);
        LinkedList<String> big_word = input_l;
        int batch = input_l.size();
        for (int i = 0; i < batch; i++) {
            String word = big_word.get(i);
            if (word != null) {
                MutableLong count = counts.get(word);

                if (count == null) {
                    count = new MutableLong(0);
                    counts.put(word, count);
                }
                count.increment();
                //if(curr%100==0){
                collector.emit(input, new Values(word, count.get()));
                //}
            }
        }
        collector.ack(input);
    }
}
