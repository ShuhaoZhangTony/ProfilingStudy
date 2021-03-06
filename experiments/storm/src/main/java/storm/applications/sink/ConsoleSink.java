package storm.applications.sink;

import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author mayconbordin
 */
public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);
    private int count = 0;


    @Override
    public void execute(Tuple input) {
        System.out.println(formatter.format(input) + ":" + count++);
        collector.ack(input);
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

}
