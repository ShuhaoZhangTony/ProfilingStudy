package storm.applications.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang3.StringUtils;
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
import java.util.Map;

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
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
        super.prepare(stormConf,context,collector);
        long threadId = Thread.currentThread().getId();
        LOG.info("Thread # " + threadId + " is doing this task");
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
        String[] words = input.getString(0).split(splitregex);
        for (String word : words) {
            if (!StringUtils.isBlank(word))
                collector.emit(input, new Values(word));
        }
        collector.ack(input);
    }
}
