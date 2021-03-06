package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.TrafficMonitoringConstants.Field;
import flink.applications.model.gis.Road;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn
 */
public class BatchSpeedCalculatorBolt extends AbstractBolt {
    private Map<Integer, Road> roads;

    @Override
    public void initialize() {
        roads = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {

        //input..
        LinkedList<String> batch_cardID = (LinkedList<String>) input.getValue(0);
        int batch = batch_cardID.size();
        LinkedList<DateTime> batch_date = (LinkedList<DateTime>) input.getValue(1);
        LinkedList<Boolean> batch_occ = (LinkedList<Boolean>) input.getValue(2);
        LinkedList<Integer> batch_speed = (LinkedList<Integer>) input.getValue(3);
        LinkedList<Integer> batch_bearing = (LinkedList<Integer>) input.getValue(4);
        LinkedList<Double> batch_lat = (LinkedList<Double>) input.getValue(5);
        LinkedList<Double> batch_lon = (LinkedList<Double>) input.getValue(6);
        LinkedList<Integer> batch_roadID = (LinkedList<Integer>) input.getValue(7);

        for (int i = 0; i < batch; i++) {
            int roadID = batch_roadID.get(i);
            int speed = batch_speed.get(i);

            int averageSpeed = 0;
            int count = 0;

            if (!roads.containsKey(roadID)) {
                Road road = new Road(roadID);
                road.addRoadSpeed(speed);
                road.setCount(1);
                road.setAverageSpeed(speed);

                roads.put(roadID, road);
                averageSpeed = speed;
                count = 1;
            } else {
                Road road = roads.get(roadID);

                int sum = 0;

                if (road.getRoadSpeedSize() < 2) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);

                    for (int it : road.getRoadSpeed()) {
                        sum += it;
                    }

                    averageSpeed = (int) ((double) sum / (double) road.getRoadSpeedSize());
                    road.setAverageSpeed(averageSpeed);
                    count = road.getRoadSpeedSize();
                } else {
                    double avgLast = roads.get(roadID).getAverageSpeed();
                    double temp = 0;

                    for (int it : road.getRoadSpeed()) {
                        sum += it;
                        temp += Math.pow((it - avgLast), 2);
                    }

                    int avgCurrent = (int) ((sum + speed) / ((double) road.getRoadSpeedSize() + 1));
                    temp = (temp + Math.pow((speed - avgLast), 2)) / (road.getRoadSpeedSize());
                    double stdDev = Math.sqrt(temp);

                    if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                        road.incrementCount();
                        road.addRoadSpeed(speed);
                        road.setAverageSpeed(avgCurrent);

                        averageSpeed = avgCurrent;
                        count = road.getRoadSpeedSize();
                    }
                }
            }

            collector.emit(input, new Values(new Date(), roadID, averageSpeed, count));
        }
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.NOW_DATE, Field.ROAD_ID, Field.AVG_SPEED, Field.COUNT);
    }
}
