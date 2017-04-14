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
package flink.applications.topology.special_LRF.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import flink.applications.topology.special_LRF.TopologyControl;
import flink.applications.topology.special_LRF.types.internal.AvgSpeedTuple;
import flink.applications.topology.special_LRF.types.internal.AvgVehicleSpeedTuple;
import flink.applications.topology.special_LRF.types.util.SegmentIdentifier;
import flink.applications.topology.special_LRF.util.AvgValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * {@link AverageSpeedBolt} computes the average speed over all vehicle within an express way segment (single direction)
 * every minute. The input is expected to be of type {@link AvgVehicleSpeedTuple}, to be ordered by timestamp, and must
 * be grouped by {@link SegmentIdentifier}. A new average speed computation is trigger each 60 seconds (ie, changing
 * 'minute number' [see Time.getMinute(short)]).<br />
 * <br />
 * <strong>Input schema:</strong> {@link AvgVehicleSpeedTuple}<br />
 * <strong>Output schema:</strong> {@link AvgSpeedTuple}
 *
 * @author mjsax
 */
public class AverageSpeedBolt extends BaseRichBolt {
    private static final long serialVersionUID = -8258719764537430323L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AverageSpeedBolt.class);
    /**
     * Internally (re)used object to access individual attributes.
     */
    private final AvgSpeedTuple inputTuple = new AvgSpeedTuple();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segment = new SegmentIdentifier();
    /**
     * Maps each segment to its average speed value.
     */
    private final Map<SegmentIdentifier, AvgValue> avgSpeedsMap = new HashMap<SegmentIdentifier, AvgValue>();
    /**
     * The storm provided output collector.
     */
    private OutputCollector collector;
    /**
     * The currently processed 'minute number'.
     */
    private short currentMinute = 1;


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        this.inputTuple.clear();
        this.inputTuple.addAll(input.getValues());
        LOGGER.trace("average Speed this.inputTuple:" + this.inputTuple.toString());

        short minute = this.inputTuple.getMinuteNumber().shortValue();
        int avgVehicleSpeed = this.inputTuple.getAvgSpeed().intValue();
        this.segment.set(this.inputTuple);

        assert (minute >= this.currentMinute);

        if (minute > this.currentMinute) {
            // emit all values for last minute
            // (because input tuples are ordered by ts (ie, minute number), we can close the last minute safely)
            for (Entry<SegmentIdentifier, AvgValue> entry : this.avgSpeedsMap.entrySet()) {
                SegmentIdentifier segId = entry.getKey();

                // Minute-Number, X-Way, Segment, Direction, Avg(speed)
                this.collector.emit(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
                        new AvgSpeedTuple
                                (new Short(this.currentMinute), segId.getXWay(), segId
                                        .getSegment(), segId.getDirection(), entry.getValue().getAverage()));
            }

            this.avgSpeedsMap.clear();
            this.currentMinute = minute;
        }

        AvgValue segAvg = this.avgSpeedsMap.get(this.segment);
        if (segAvg == null) {
            segAvg = new AvgValue(avgVehicleSpeed);
            this.avgSpeedsMap.put(this.segment.copy(), segAvg);
        } else {
            segAvg.updateAverage(avgVehicleSpeed);
        }

        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, AvgSpeedTuple.getSchema());
    }

}
