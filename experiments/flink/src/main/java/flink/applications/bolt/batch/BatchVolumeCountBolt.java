package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.LogProcessingConstants.Conf;
import flink.applications.constants.LogProcessingConstants.Field;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt will count number of log events per minute
 */
public class BatchVolumeCountBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchVolumeCountBolt.class);

    private CircularFifoBuffer buffer;
    private Map<Long, MutableLong> counts;

    @Override
    public void initialize() {
        int windowSize = config.getInt(Conf.VOLUME_COUNTER_WINDOW, 60);

        buffer = new CircularFifoBuffer(windowSize);
        counts = new HashMap<>(windowSize);
    }

    @Override
    public void execute(Tuple input) {

        long[] Ominute = ((long[]) input.getValue(2));
        int batch = Ominute.length;
        for (int i = 0; i < batch; i++) {
            long minute = Ominute[i];
            MutableLong count = counts.get(minute);
            if (count == null) {
                if (buffer.isFull()) {
                    long oldMinute = (Long) buffer.remove();
                    counts.remove(oldMinute);
                }

                count = new MutableLong(1);
                counts.put(minute, count);
                buffer.add(minute);
            } else {
                count.increment();
            }
            collector.emit(input, new Values(minute, count.longValue()));
        }
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP_MINUTES, Field.COUNT);
    }
}
