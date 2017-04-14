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
package storm.applications.topology.special_LRF.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.hooks.BoltMeterHook;
import storm.applications.topology.special_LRF.TopologyControl;
import storm.applications.topology.special_LRF.types.AccidentNotification;
import storm.applications.topology.special_LRF.types.PositionReport;
import storm.applications.topology.special_LRF.types.internal.AccidentTuple;
import storm.applications.topology.special_LRF.types.util.ISegmentIdentifier;
import storm.applications.topology.special_LRF.types.util.SegmentIdentifier;
import storm.applications.topology.special_LRF.util.Constants;
import storm.applications.util.config.Configuration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static storm.applications.util.config.Configuration.METRICS_ENABLED;


/**
 * {@link AccidentNotificationBolt} notifies vehicles approaching an accident (ie, are at most 4 segments upstream of
 * the accident) to allow them to exit the express way. Vehicles are notified about accidents that occurred in the
 * minute before the current {@link PositionReport} (ie, 'minute number' [see Time.getMinute(short)])) and as long as
 * the accident was not cleared and only once per segment (ie, each time a new segment is entered).<br />
 * <br />
 * {@link AccidentNotificationBolt} processes two input streams. The first input is expected to be of type
 * {@link PositionReport} and must be grouped by vehicle id. The second input is expected to be of type
 * {@link AccidentTuple} and must be broadcasted. Both inputs most be ordered by time (ie, timestamp for
 * {@link PositionReport} and minute number for {@link AccidentTuple}). It is further assumed, that all
 * {@link AccidentTuple}s with a <em>smaller</em> minute number than a {@link PositionReport} tuple are delivered
 * <em>before</em> those {@link PositionReport}s.<br />
 * <br />
 * This implementation assumes, that {@link PositionReport}s are delivered via a stream called
 * {@link TopologyControl#POSITION_REPORTS_STREAM_ID}. Input tuples of all other streams are assumed to be
 * {@link AccidentTuple}s.<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport} and {@link AccidentTuple}<br />
 * <strong>Output schema:</strong> {@link AccidentNotification}
 *
 * @author mjsax
 * @author trillian
 */
public class AccidentNotificationBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AccidentNotificationBolt.class);
    /**
     * Contains all vehicle IDs and segment of the last {@link PositionReport} to allow skipping already sent
     * notifications (there's only one notification per segment per vehicle).
     */
    private final Map<Integer, Short> allCars = new HashMap<Integer, Short>();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private final PositionReport inputPositionReport = new PositionReport();
    /**
     * Internally (re)used object to access individual attributes.
     */
    private final AccidentTuple inputAccidentTuple = new AccidentTuple();
    /**
     * Internally (re)used object.
     */
    private final SegmentIdentifier segmentToCheck = new SegmentIdentifier();
    /**
     * The storm provided output collector.
     */
    private OutputCollector collector;
    /**
     * Buffer for accidents.
     */
    private Set<ISegmentIdentifier> currentMinuteAccidents = new HashSet<ISegmentIdentifier>();
    /**
     * Buffer for accidents.
     */
    private Set<ISegmentIdentifier> previousMinuteAccidents = new HashSet<ISegmentIdentifier>();
    /**
     * The currently processed 'minute number'.
     */
    private int currentMinute = -1;


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
        if (input.getSourceStreamId().equals(TopologyControl.POSITION_REPORTS_STREAM_ID)) {
            this.inputPositionReport.clear();
            this.inputPositionReport.addAll(input.getValues());
            LOGGER.trace("ACCNotification,this.inputPositionReport:" + this.inputPositionReport.toString());

            this.checkMinute(this.inputPositionReport.getMinuteNumber());

            if (this.inputPositionReport.isOnExitLane()) {
                this.collector.ack(input);
                return;
            }

            final Short currentSegment = this.inputPositionReport.getSegment();
            final Integer vid = this.inputPositionReport.getVid();
            final Short previousSegment = this.allCars.put(vid, currentSegment);
            if (previousSegment != null && currentSegment.shortValue() == previousSegment.shortValue()) {
                this.collector.ack(input);
                return;
            }

            // upstream is either larger or smaller of current segment
            final Short direction = this.inputPositionReport.getDirection();
            final short dir = direction.shortValue();
            // EASTBOUND == 0 => diff := 1
            // WESTBOUNT == 1 => diff := -1
            final short diff = (short) -(dir - 1 + ((dir + 1) / 2));
            assert (dir == Constants.EASTBOUND.shortValue() ? diff == 1 : diff == -1);

            final Integer xway = this.inputPositionReport.getXWay();
            final short curSeg = currentSegment.shortValue();

            this.segmentToCheck.setXWay(xway);
            this.segmentToCheck.setDirection(direction);

            for (int i = 0; i <= 4; ++i) {
                final short nextSegment = (short) (curSeg + (diff * i));
                assert (dir == Constants.EASTBOUND.shortValue() ? nextSegment >= curSeg : nextSegment <= curSeg);

                this.segmentToCheck.setSegment(new Short(nextSegment));

                if (this.previousMinuteAccidents.contains(this.segmentToCheck)) {
                    // TODO get accurate emit time...
                    this.collector.emit(TopologyControl.ACCIDENTS_NOIT_STREAM_ID, new AccidentNotification(this.inputPositionReport.getTime(),
                            this.inputPositionReport.getTime(), this.segmentToCheck.getSegment(), vid));

                    break; // send a notification for the closest accident only
                }
            }
        } else {
            this.inputAccidentTuple.clear();
            this.inputAccidentTuple.addAll(input.getValues());
//			LOGGER.trace(this.inputAccidentTuple.toString());
            this.checkMinute(this.inputAccidentTuple.getMinuteNumber().shortValue());
            assert (this.inputAccidentTuple.getMinuteNumber().shortValue() == this.currentMinute);

            this.currentMinuteAccidents.add(new SegmentIdentifier(this.inputAccidentTuple));
        }

        this.collector.ack(input);
    }

    private void checkMinute(short minute) {
        assert (minute >= this.currentMinute);

        if (minute > this.currentMinute) {
            LOGGER.trace("New minute: {}", new Short(minute));
            this.currentMinute = minute;
            this.previousMinuteAccidents = this.currentMinuteAccidents;
            this.currentMinuteAccidents = new HashSet<ISegmentIdentifier>();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyControl.ACCIDENTS_NOIT_STREAM_ID, AccidentNotification.getSchema());
    }

}
