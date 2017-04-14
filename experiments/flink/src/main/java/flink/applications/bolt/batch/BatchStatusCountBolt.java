package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.LogProcessingConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class BatchStatusCountBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchStatusCountBolt.class);
    private Map<Integer, Integer> counts;

    @Override
    public void initialize() {
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {

        int[] ORESPONSE = (int[]) input.getValue(4);
        int batch = ORESPONSE.length;
        try {
            for (int i = 0; i < batch; i++) {
                int statusCode = ORESPONSE[i];
                int count = 0;

                if (counts.containsKey(statusCode)) {
                    count = counts.get(statusCode);
                }

                count++;
                counts.put(statusCode, count);

                collector.emit(input, new Values(statusCode, count));
            }
        } catch (Exception e) {
            System.out.println("");
        }
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.RESPONSE, Field.COUNT);
    }
}
