package flink.applications.bolt;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.StreamGrepConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamGrepBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StreamGrepBolt.class);
    private static final String splitregex = ",";

    @Override
    public Fields getDefaultFields() {
        return new Fields(StreamGrepConstants.Field.TEXT);
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
        String sentence = input.getStringByField(StreamGrepConstants.Field.TEXT);
        if (match(sentence.split(splitregex)))
            collector.emit(input, new Values(sentence));
        collector.ack(input);
    }

}
