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
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.hooks.BoltMeterHook;
import storm.applications.topology.special_LRF.TopologyControl;
import storm.applications.topology.special_LRF.types.internal.AvgSpeedTuple;
import storm.applications.topology.special_LRF.types.internal.AvgVehicleSpeedTuple;
import storm.applications.topology.special_LRF.types.internal.LavTuple;
import storm.applications.topology.special_LRF.types.util.SegmentIdentifier;
import storm.applications.util.config.Configuration;

import java.util.*;
import java.util.Map.Entry;

import static storm.applications.util.config.Configuration.METRICS_ENABLED;


/**
 * {@link BatchLatestAverageVelocityBolt} computes the "latest average velocity" (LAV), ie, the average speed over all
 * vehicle within an express way-segment (single direction), over the last five minutes (see Time.getMinute(short)]).
 * The input is expected to be of type {@link AvgSpeedTuple}, to be ordered by timestamp, and must be grouped by
 * {@link SegmentIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link AvgSpeedTuple}<br />
 * <strong>Output schema:</strong> {@link LavTuple} ( (stream: {@link TopologyControl#LAVS_STREAM_ID})
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class BatchLatestAverageVelocityBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchLatestAverageVelocityBolt.class);

    /**
     * Holds the (at max) last five average speed value for each segment.
     */
    private final Map<SegmentIdentifier, List<Integer>> averageSpeedsPerSegment = new HashMap<SegmentIdentifier, List<Integer>>();

    /**
     * Holds the (at max) last five minute numbers for each segment.
     */
    private final Map<SegmentIdentifier, List<Short>> minuteNumbersPerSegment = new HashMap<SegmentIdentifier, List<Short>>();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private final AvgVehicleSpeedTuple inputTuple = new AvgVehicleSpeedTuple();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentIdentifier = new SegmentIdentifier();
    /**
     * Storm provided output collector.
     */
    private OutputCollector collector;
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
        this.collector = collector;
        Configuration config = Configuration.fromMap(stormConf);
        if (config.getBoolean(METRICS_ENABLED, false)) {
            context.addTaskHook(new BoltMeterHook());
        }
    }

    @Override
    public void execute(Tuple input) {

        //get input:
        LinkedList<AvgVehicleSpeedTuple> input_l = (LinkedList<AvgVehicleSpeedTuple>) input.getValue(0);

        //output :
        LinkedList<LavTuple> lavo = new LinkedList<LavTuple>();

        for (int i = 0; i < input_l.size(); i++) {

            this.inputTuple.clear();
            this.inputTuple.addAll(input_l.get(i));
            LOGGER.trace(this.inputTuple.toString());

            Short minuteNumber = this.inputTuple.getMinute();
            short m = minuteNumber.shortValue();

            assert (m >= this.currentMinute);

            if (m > this.currentMinute) {
                // each time we step from one minute to another, we need to check the previous time range for "unfinished"
                // open windows; this can happen, if there is not AvgSpeedTuple for a segment in the minute before the
                // current one; we need to truncate all open windows and compute LAV values for each open segment

                short nextMinute = this.currentMinute;
                // a segment can have multiple consecutive missing AvgSpeedTuple
                // (for example, if no more cars drive on a segment)
                while (nextMinute++ < m) {
                    Iterator<Entry<SegmentIdentifier, List<Short>>> it = this.minuteNumbersPerSegment.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<SegmentIdentifier, List<Short>> e = it.next();
                        SegmentIdentifier sid = e.getKey();
                        List<Short> latestMinuteNumber = e.getValue();

                        if (latestMinuteNumber.get(latestMinuteNumber.size() - 1).shortValue() < nextMinute - 1) {
                            List<Integer> latestAvgSpeeds = this.averageSpeedsPerSegment.get(sid);

                            // truncate window if entry is more than 5 minutes older than nextMinute
                            // (can be at max one entry)
                            if (latestMinuteNumber.get(0).shortValue() < nextMinute - 5) {
                                latestAvgSpeeds.remove(0);
                                latestMinuteNumber.remove(0);
                            }

                            if (latestAvgSpeeds.size() > 0) {
                                Integer lav = this.computeLavValue(latestAvgSpeeds);
//                                this.collector.emit(TopologyControl.LAVS_STREAM_ID,
//                                        new LavTuple(new Short(nextMinute), sid.getXWay(), sid.getSegment(),
//                                                sid.getDirection(), lav));
                                lavo.add(new LavTuple(new Short(nextMinute), sid.getXWay(), sid.getSegment(),
                                        sid.getDirection(), lav));
                            } else {
                                // remove empty window completely
                                it.remove();
                                this.averageSpeedsPerSegment.remove(sid);
                            }
                        }
                    }
                }
                this.currentMinute = m;
            }

            this.segmentIdentifier.set(this.inputTuple);
            List<Integer> latestAvgSpeeds = this.averageSpeedsPerSegment.get(this.segmentIdentifier);
            List<Short> latestMinuteNumber = this.minuteNumbersPerSegment.get(this.segmentIdentifier);

            if (latestAvgSpeeds == null) {
                latestAvgSpeeds = new LinkedList<Integer>();
                this.averageSpeedsPerSegment.put(this.segmentIdentifier.copy(), latestAvgSpeeds);
                latestMinuteNumber = new LinkedList<Short>();
                this.minuteNumbersPerSegment.put(this.segmentIdentifier.copy(), latestMinuteNumber);
            }
            latestAvgSpeeds.add(this.inputTuple.getAvgSpeed());
            latestMinuteNumber.add(minuteNumber);

            // discard all values that are more than 5 minutes older than current minute
            while (latestAvgSpeeds.size() > 1) {
                if (latestMinuteNumber.get(0).shortValue() < m - 4) {
                    latestAvgSpeeds.remove(0);
                    latestMinuteNumber.remove(0);
                } else {
                    break;
                }
            }
            Integer lav = this.computeLavValue(latestAvgSpeeds);
            lavo.add(new LavTuple(new Short((short) (m + 1)), this.segmentIdentifier.getXWay(), this.segmentIdentifier
                    .getSegment(), this.segmentIdentifier.getDirection(), lav));
        }
        if (lavo.size() > 0)
            this.collector.emit(
                    TopologyControl.LAVS_STREAM_ID, new Values(lavo)
            );

        this.collector.ack(input);
    }

    private Integer computeLavValue(List<Integer> latestAvgSpeeds) {
        int speedSum = 0;
        int valueCount = 0;
        for (Integer speed : latestAvgSpeeds) {
            speedSum += speed.intValue();
            ++valueCount;
        }

        return new Integer(speedSum / valueCount);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyControl.LAVS_STREAM_ID, new Fields(TopologyControl.Field.LavTuple));
    }

}
