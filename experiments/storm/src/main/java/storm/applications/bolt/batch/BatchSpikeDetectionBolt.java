package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.SpikeDetectionConstants.Conf;
import storm.applications.constants.SpikeDetectionConstants.Field;

/**
 * Emits a tuple if the current value surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class BatchSpikeDetectionBolt extends AbstractBolt {
    private double spikeThreshold;

    @Override
    public void initialize() {
        spikeThreshold = config.getDouble(Conf.SPIKE_DETECTOR_THRESHOLD, 0.03d);
    }

    @Override
    public void execute(Tuple input) {
        //input..
        String[] batch_MOTEID_FIELD = (String[]) input.getValue(0);
        int batch = batch_MOTEID_FIELD.length;
        Double MA[] = (Double[]) input.getValue(1);
        Double batch_key[] = (Double[]) input.getValue(2);
        //output..

        for (int i = 0; i < batch; i++) {
            String deviceID = batch_MOTEID_FIELD[i];
            double movingAverageInstant = MA[i];
            double nextDouble = batch_key[i];

            if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
                collector.emit(input, new Values(deviceID, movingAverageInstant, nextDouble, "spike detected"));
            }
        }
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE, Field.MESSAGE);
    }
}