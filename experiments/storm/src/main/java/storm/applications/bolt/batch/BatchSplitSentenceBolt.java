package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.WordCountConstants.Field;

import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class BatchSplitSentenceBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchSplitSentenceBolt.class);
    long start = 0, end = 0;
    boolean update = false;
    private int executionLatency = 0;
    private int curr = 0, precurr = 0;
    private int batch;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    /*
    * Used for cost-aware grouping...*/
//    @Override
//    public Map<String, Object> getComponentConfiguration() {
//        Map<String, Object> conf = new HashMap<>();
//        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
//        return conf;
//    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        return conf;
    }

    protected void update_statistics(int number_ofExecuted) throws IOException {

        File f = new File("split" + this.context.getThisTaskId() + ".txt");

        FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        CharBuffer charBuf = b.asCharBuffer();

        char[] string = String.valueOf(number_ofExecuted).toCharArray();
        charBuf.put(string);

    }

    /*
    * Used for cost-aware grouping*/
//    @Override
//    public void execute(Tuple input) {
//        if (TupleUtils.isTickTuple(input)) {
//            try {
//                update_statistics(curr - precurr);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            precurr = curr;
//        } else {
//            String[] words = input.getString(0).split(splitregex);
//            for (String word : words) {
//                if (!StringUtils.isBlank(word))
//                    collector.emit(input, new Values(word));
//                collector.ack(input);
//            }
//        }
//        curr++;
//    }
    @Override
    public void execute(Tuple input) {

        LinkedList<String> batchedSentences = new LinkedList<>();
        if (input.getValue(0) instanceof LinkedList) {
            batchedSentences = (LinkedList<String>) input.getValue(0);
            batch = batchedSentences.size();
        } else {
            batchedSentences.add((String) input.getValue(0));
            batch = 1;
        }


        String[][] prepareSentences = new String[10][batch];
        for (int i = 0; i < batch; i++) {
            String input_sentence = batchedSentences.get(i);
            //LOG.info(input_sentence);
            if (input_sentence != null) {
                String[] temp = input_sentence.split(splitregex);
                for (int j = 0; j < 10; j++) {
                    //LOG.info(temp[j]);
                    prepareSentences[j][i] = temp[j];
                }
            }
        }

        for (int i = 0; i < 10; i++) {
            LinkedList<String> big_word = new LinkedList<>();
            for (int j = 0; j < batch; j++) {
                String outputWord = prepareSentences[i][j];
                if (outputWord != null)
                    big_word.add(prepareSentences[i][j]);
            }
            collector.emit(input, new Values(big_word));
        }
        collector.ack(input);
    }
}
