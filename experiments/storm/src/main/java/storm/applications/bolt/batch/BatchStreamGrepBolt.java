package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.StreamGrepConstants;

import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class BatchStreamGrepBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchStreamGrepBolt.class);
    long start = 0, end = 0;
    boolean update = false;
    private int executionLatency = 0;
    private int curr = 0, precurr = 0;
    private int batch;

    @Override
    public Fields getDefaultFields() {
        return new Fields(StreamGrepConstants.Field.TEXT);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        return conf;
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

        LinkedList<String> batchedSentences = (LinkedList<String>) input.getValue(0);
//        if (input.getValue(0) instanceof LinkedList) {
//
//        } else {
//            batchedSentences.add((String) input.getValue(0));
//        }
        for (String sentence : batchedSentences) {
            if (match(sentence.split(splitregex)))
                collector.emit(input, new Values(sentence));
        }
//        for (int i = 0; i < batch; i++) {
//            String input_sentence = batchedSentences.get(i);
//            //LOG.info(input_sentence);
//            if (input_sentence != null) {
//                String[] temp = input_sentence.split(splitregex);
//                for (int j = 0; j < 10; j++) {
//                    //LOG.info(temp[j]);
//                    prepareSentences[j][i] = temp[j];
//                }
//            }
//        }

        collector.ack(input);
    }
}
