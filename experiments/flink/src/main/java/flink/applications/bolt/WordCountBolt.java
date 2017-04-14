package flink.applications.bolt;

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

public class WordCountBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt.class);
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
        //curr++;
        String word = input.getStringByField(Field.WORD);
       // MutableLong count = counts.get(word);

        //if (count == null) {
        //    count = new MutableLong(0);
           // counts.put(word, count);
       // }
        //count.increment();
        //collector.emit(input, new Values(word, count.get()));
        collector.emit(input, new Values(word, 0));
        collector.ack(input);
    }

}
