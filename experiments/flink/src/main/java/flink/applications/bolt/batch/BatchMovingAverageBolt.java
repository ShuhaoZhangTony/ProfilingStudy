package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.SpikeDetectionConstants.Conf;
import flink.applications.constants.SpikeDetectionConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Calculates the average over a window for distinct elements.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class BatchMovingAverageBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchMovingAverageBolt.class);

    private int movingAverageWindow;
    private Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private Map<String, Double> deviceIDtoSumOfEvents;

    @Override
    public void initialize() {
        movingAverageWindow = config.getInt(Conf.MOVING_AVERAGE_WINDOW, 1000);
        deviceIDtoStreamMap = new HashMap<>();
        deviceIDtoSumOfEvents = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        //input..
        String[] batch_MOTEID_FIELD = (String[]) input.getValue(0);
        int batch = batch_MOTEID_FIELD.length;
        //Date batch_date[] = (Date[]) input.getValue(1);
        Double batch_key[] = (Double[]) input.getValue(2);
        //output..
        Double MA[] = new Double[batch];

        for (int i = 0; i < batch; i++) {
            String deviceID = batch_MOTEID_FIELD[i];
            double nextDouble = batch_key[i];
            MA[i] = movingAverage(deviceID, nextDouble);
        }

        collector.emit(input, new Values(batch_MOTEID_FIELD, MA, batch_key));
        collector.ack(input);
    }

    public double movingAverage(String deviceID, double nextDouble) {
        LinkedList<Double> valueList = new LinkedList<>();
        double sum = 0.0;

        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow - 1) {
                double valueToRemove = valueList.removeFirst();
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum / valueList.size();
        } else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE);
    }
}