package flink.applications.sink;

import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mayconbordin
 */
public class ConsoleSink2 extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink2.class);
    private int count = 0;


    @Override
    public void execute(Tuple input) {
        //   	System.out.println(formatter.format(input)+":"+count++);
//    	count++;
        collector.ack(input);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
