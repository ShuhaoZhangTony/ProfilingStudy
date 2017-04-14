package storm.applications.bolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.applications.bolt.base.AbstractBolt;

import static storm.applications.constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class VisitStatsBolt extends AbstractBolt {
    private int total = 0;
    private int uniqueCount = 0;

    @Override
    public void initialize() {
    }

    @Override
    public void execute(Tuple input) {
        boolean unique = Boolean.parseBoolean(input.getStringByField(Field.UNIQUE));
        total++;
        if (unique) uniqueCount++;

        collector.emit(input, new Values(total, uniqueCount));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TOTAL_COUNT, Field.TOTAL_UNIQUE);
    }
}
