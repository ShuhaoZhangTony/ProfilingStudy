package storm.applications.bolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.WordCountConstants.Field;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class StreamGrepBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StreamGrepBolt.class);
    private static final String splitregex = ",";

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TEXT);
    }

    /**
     * It checks each word in the sentence against several dummy rules.
     * Rule) the hashcode should be larger than 0.
     *
     * @param words
     * @return
     */
    private boolean match(String[] words) {


        for (String word : words) {
            if (words.hashCode() < 0 && word.hashCode() < 0) return false;
        }
        return true;
    }

    @Override
    public void execute(Tuple input) {

        //context.getThisComponentId();

        //curr++;
        String sentence = input.getStringByField(Field.TEXT);
        if (match(sentence.split(splitregex)))
            collector.emit(input, new Values(sentence));
        collector.ack(input);
    }

}
