/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package storm.applications.topology.special_LRF.bolt.batch;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.LinearRoadFConstants;
import storm.applications.hooks.BoltMeterHook;
import storm.applications.topology.special_LRF.TopologyControl;
import storm.applications.topology.special_LRF.types.PositionReport;
import storm.applications.topology.special_LRF.types.internal.AvgVehicleSpeedTuple;
import storm.applications.topology.special_LRF.types.util.SegmentIdentifier;
import storm.applications.topology.special_LRF.util.AvgValue;
import storm.applications.topology.special_LRF.util.Time;
import storm.applications.util.Multi_Key_value_Map;
import storm.applications.util.config.Configuration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import static storm.applications.util.config.Configuration.METRICS_ENABLED;


/**
 * {@link BatchAverageVehicleSpeedBolt} computes the average speed of a vehicle within an express way-segment (single
 * direction) every minute. The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and
 * must be grouped by vehicle. A new average speed computation is trigger each time a vehicle changes the express way,
 * segment or direction as well as each 60 seconds (ie, changing 'minute number' [see {@link Time#getMinute(long)}]).<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link AvgVehicleSpeedTuple}
 *
 * @author msoyka
 * @author mjsax
 */
public class BatchAverageVehicleSpeedBolt extends BaseRichBolt {
    private final static long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchAverageVehicleSpeedBolt.class);
    /**
     * Internally (re)used object to access individual attributes.
     */
    private final PositionReport inputPositionReport = new PositionReport();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each vehicle to its average speed value that corresponds to the current 'minute number' and specified
     * segment.
     */
    private final Map<Integer, Pair<AvgValue, SegmentIdentifier>> avgSpeedsMap = new HashMap<Integer, Pair<AvgValue, SegmentIdentifier>>();
    /**
     * The storm provided output collector.
     */
    private OutputCollector collector;
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;
    private Multi_Key_value_Map cache;
    private int worker;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
        this.collector = collector;
        Configuration config = Configuration.fromMap(stormConf);
        if (config.getBoolean(METRICS_ENABLED, false)) {
            context.addTaskHook(new BoltMeterHook());
        }

        //cache = new Multi_Key_value_Map();
        worker = config.getInt(LinearRoadFConstants.Conf.LatestAverageVelocityThreads);
    }

    @Override
    public void execute(Tuple update) {

        LinkedList<PositionReport> input_l = (LinkedList<PositionReport>) update.getValue(0);

        //output :
       // LinkedList<AvgVehicleSpeedTuple> avg = new LinkedList<AvgVehicleSpeedTuple>();
        cache = new Multi_Key_value_Map();

        for (int i = 0; i < input_l.size(); i++) {
            this.inputPositionReport.clear();
            this.inputPositionReport.addAll(input_l.get(i));
            LOGGER.trace(this.inputPositionReport.toString());

            Integer vid = this.inputPositionReport.getVid();
            short minute = this.inputPositionReport.getMinuteNumber();
            int speed = this.inputPositionReport.getSpeed().intValue();
            this.segment.set(this.inputPositionReport);

//            assert (minute >= this.currentMinute);

            //           if (minute > this.currentMinute) {
            // emit all values for last minute
            // (because input tuples are ordered by ts, we can close the last minute safely)
            for (Entry<Integer, Pair<AvgValue, SegmentIdentifier>> entry : this.avgSpeedsMap.entrySet()) {
                Pair<AvgValue, SegmentIdentifier> value = entry.getValue();
                SegmentIdentifier segId = value.getRight();

                // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
//                    this.collector.emit(
//                            TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
//                            new AvgVehicleSpeedTuple(entry.getKey(),
//                                    new Short(this.currentMinute), segId
//                                    .getXWay(), segId.getSegment(), segId.getDirection(), value.getLeft().getAverage()));
  /*TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
                                    TopologyControl.DIRECTION_FIELD_NAME*/
                cache.put(worker, new AvgVehicleSpeedTuple(entry.getKey(),
                                new Short(this.currentMinute),
                                segId.getXWay(),
                                segId.getSegment(),
                                segId.getDirection(), value.getLeft().getAverage()),

                        //String.valueOf(segId.getXWay()),
                        String.valueOf(segId.getSegment())
                        //, String.valueOf(segId.getDirection())
                );

//                avg.add(new AvgVehicleSpeedTuple(entry.getKey(),
//                        new Short(this.currentMinute), segId
//                        .getXWay(), segId.getSegment(), segId.getDirection(), value.getLeft().getAverage()));

            }

            this.avgSpeedsMap.clear();
            this.currentMinute = minute;
//            }

            Pair<AvgValue, SegmentIdentifier> vehicleEntry = this.avgSpeedsMap.get(vid);
            if (vehicleEntry == null) {
                vehicleEntry = new MutablePair<AvgValue, SegmentIdentifier>(new AvgValue(speed), this.segment.copy());
                this.avgSpeedsMap.put(vid, vehicleEntry);
            } else {
                vehicleEntry.getLeft().updateAverage(speed);
            }

            if (vehicleEntry != null && !vehicleEntry.getRight().equals(this.segment)) {
                SegmentIdentifier segId = vehicleEntry.getRight();

                //build batch output.
                // VID, Minute-Number, X-Way, Segment, Direction, Avg(speed)
//                this.collector.emit(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
//                        new AvgVehicleSpeedTuple
//                                (
//                                        vid,
//                                        new Short(this.currentMinute), segId.getXWay(), segId
//                                        .getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage()));

                cache.put(worker, new AvgVehicleSpeedTuple
                                (
                                        vid,
                                        new Short(this.currentMinute), segId.getXWay(), segId
                                        .getSegment(), segId.getDirection(), vehicleEntry.getLeft().getAverage()),
                        // String.valueOf(segId.getXWay()),
                        String.valueOf(segId.getSegment())
                        //,
                        //        String.valueOf(segId.getDirection())
                );
            }
        }

        //batch emit.
        cache.emit(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, this.collector);
        this.collector.ack(update);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
                new Fields(TopologyControl.Field.AvgVehicleSpeedTuple, TopologyControl.Field.AvgVehicleSpeedTuple_Key));
    }

}
