package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.WordCountConstants.Field;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class SplitSentenceBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt.class);
    long start = 0, end = 0;
    boolean update = false;
    private int executionLatency = 0;
    private int curr = 0, precurr = 0;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    protected void update_statistics(int number_ofExecuted) throws IOException {

        File f = new File("split" + this.context.getThisTaskId() + ".txt");

        FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        CharBuffer charBuf = b.asCharBuffer();

        char[] string = String.valueOf(number_ofExecuted).toCharArray();
        charBuf.put(string);

        //System.out.println("Waiting for client.");
        //while (charBuf.get(0) != '\0') ;
        //System.out.println("Finished waiting.");
    }

    @Override
    public void execute(Tuple input) {
        String[] words = new String[0];
        //System.out.println(input.toString());
//        LinkedList value = (LinkedList) input.getValue(0);
        //System.out.println(value);
        words = ((String) input.getValue(0)).split(splitregex);
        //words = (String[]) value.toArray(new String[value.size()]);

        for (String word : words) {
            if (!StringUtils.isBlank(word))
                collector.emit(input, new Values(word));
        }
        collector.ack(input);
    }
}
