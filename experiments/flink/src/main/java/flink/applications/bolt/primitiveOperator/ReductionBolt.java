package flink.applications.bolt.primitiveOperator;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.primitiveOperator.AggregateConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by szhang026 on 8/11/2015.
 * TODO: this bolt has not been fully implemented.
 */
public class ReductionBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ReductionBolt.class);
    private int emitFrequencyInSeconds;
    private boolean sawData = false;
    private long sum[];

    public ReductionBolt(int emitFrequencyInSeconds) {
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;//default is 2
    }

    @Override
    public void initialize() {
        //sum=new long[];
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(AggregateConstants.Field.LONG);
    }

    @Override
    public void execute(Tuple tuple) {
        if (isDone()) {
            //LOG.info("Received tick tuple, triggering emit of current window counts");
            if (sawData) {
                sawData = false;
                //long threadId = Thread.currentThread().getId();
                //LOG.warn("1:Thread # " + threadId + " is doing this task");
                collector.emit(new Values(sum));
            }
        } else {
            sawData = true;
            Long id = (Long) tuple.getValueByField(AggregateConstants.Field.THREAD);
            Object obj = tuple.getValueByField(AggregateConstants.Field.LONG);// in terms of a long value.
            //sum+=new Long((Integer)obj);
            //setDone(id);
            collector.ack(tuple);
        }
    }

    boolean isDone() {
        return false;
    }
}
