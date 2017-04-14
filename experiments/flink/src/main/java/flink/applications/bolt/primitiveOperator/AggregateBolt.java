package flink.applications.bolt.primitiveOperator;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.BaseConstants;
import flink.applications.constants.primitiveOperator.AggregateConstants.Field;
import flink.applications.tools.NthLastModifiedTimeTracker;
import flink.applications.tools.SlidingWindowCounter;
import flink.applications.util.stream.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by szhang026 on 8/10/2015.
 */
public class AggregateBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateBolt.class);
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
            "Actual window length is %d seconds when it should be %d seconds"
                    + " (you can safely ignore this warning during the startup phase)";
    private boolean sawData = false;
    private SlidingWindowCounter<Object> counter;
    private int windowLengthInSeconds;
    private int emitFrequencyInSeconds;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public AggregateBolt() {
        this(60);
    }

    public AggregateBolt(int emitFrequencyInSeconds) {
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;//default is 2
    }

    @Override
    public void initialize() {
        windowLengthInSeconds = config.getInt(String.format(BaseConstants.BaseConf.ROLLING_COUNT_WINDOW_LENGTH, configPrefix), 300);

        int numChunks = windowLengthInSeconds / emitFrequencyInSeconds;//10 / 2 = 5

        counter = new SlidingWindowCounter<>(numChunks);//numChunks is assigned to slots, which is parallelism region number.
        lastModifiedTracker = new NthLastModifiedTimeTracker(numChunks);
    }

    @Override
    public void execute(Tuple tuple) {
        if (TupleUtils.isTickTuple(tuple)) {
            //LOG.info("Received tick tuple, triggering emit of current window counts");
            if (sawData) {
                sawData = false;
                //long threadId = Thread.currentThread().getId();
                //LOG.warn("1:Thread # " + threadId + " is doing this task");
                emitCurrentWindowCounts();
            }
        } else {
            sawData = true;
            //long threadId = Thread.currentThread().getId();
            //LOG.warn("2:Thread # " + threadId + " is doing this task");
            countObjAndAck(tuple);//no emit.
        }
    }

    private void emitCurrentWindowCounts() {
        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();

        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }

        emit(counts);
    }

    private void emit(Map<Object, Long> counts) {
        Long sum = 0l;//min/max
        Object obj = null;
        for (Map.Entry<Object, Long> entry : counts.entrySet()) {
            obj = entry.getKey();
            Long count = entry.getValue();
            sum += new Long((Integer) obj) * count;
        }
        collector.emit(new Values(Thread.currentThread().getId(), sum));//shuffling method
    }

    private void countObjAndAck(Tuple tuple) {
        Object obj = tuple.getValue(0);// in terms of a long value.
        counter.incrementCount(obj);
        collector.ack(tuple);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.THREAD, Field.LONG);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
