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
import flink.applications.hooks.BoltMeterHook;
import flink.applications.topology.special_LRF.TopologyControl;
import flink.applications.topology.special_LRF.types.PositionReport;
import flink.applications.topology.special_LRF.types.internal.AccidentTuple;
import flink.applications.topology.special_LRF.types.util.PositionIdentifier;
import flink.applications.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static flink.applications.util.config.Configuration.METRICS_ENABLED;


/**
 * {@link AccidentDetectionBolt} registers every stopped vehicle and emits accident information for further processing.
 * The input is expected to be of type {@link PositionReport}, to be ordered by timestamp, and must be grouped by
 * {@link PositionIdentifier}.<br />
 * <br />
 * <strong>Input schema:</strong> {@link PositionReport}<br />
 * <strong>Output schema:</strong> {@link AccidentTuple} (stream: {@link TopologyControl#ACCIDENTS_STREAM_ID})
 *
 * @author msoyka
 * @author richter
 * @author mjsax
 */
public class AccidentDetectionBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOGGER = LoggerFactory.getLogger(AccidentDetectionBolt.class);
    /**
     * Internally (re)used object to access individual attributes.
     */
    private final PositionReport inputPositionReport = new PositionReport();
    /**
     * Internally (re)used object.
     */
    private final PositionIdentifier vehiclePosition = new PositionIdentifier();
    /**
     * Internally (re)used object.
     */
    private final PositionReport lastPositionReport = new PositionReport();
    /**
     * Internally (re)used object.
     */
    private final PositionIdentifier lastVehiclePosition = new PositionIdentifier();
    /**
     * Holds the last positions for each vehicle (if those positions are equal to each other).
     */
    private final Map<Integer, List<PositionReport>> lastPositions = new HashMap<Integer, List<PositionReport>>();
    /**
     * Hold all vehicles that have <em>stopped</em> within a segment.
     */
    private final Map<PositionIdentifier, Set<Integer>> stoppedCarsPerPosition = new HashMap<PositionIdentifier, Set<Integer>>();
    /**
     * The storm provided output collector.
     */
    private OutputCollector collector;
    /**
     * The currently processed 'minute number'.
     */
    private int currentMinute = -1;


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, @SuppressWarnings("hiding") OutputCollector collector) {
        this.collector = collector;
        Configuration config = Configuration.fromMap(conf);
        if (config.getBoolean(METRICS_ENABLED, false)) {
            context.addTaskHook(new BoltMeterHook());
        }
    }

    @Override
    public void execute(Tuple input) {
        this.inputPositionReport.clear();
        this.inputPositionReport.addAll(input.getValues());
        LOGGER.trace("ACCDetection,this.inputPositionReport:" + this.inputPositionReport.toString());

        Integer vid = this.inputPositionReport.getVid();
        short minute = this.inputPositionReport.getMinuteNumber();

        assert (minute >= this.currentMinute);

        if (minute > this.currentMinute) {
            this.currentMinute = minute;
            this.collector.emit(TopologyControl.ACCIDENTS_STREAM_ID, new AccidentTuple(inputPositionReport, new Short(minute)));
        }

        if (this.inputPositionReport.isOnExitLane()) {
            List<PositionReport> vehiclePositions = this.lastPositions.remove(vid);

            if (vehiclePositions != null && vehiclePositions.size() == 4) {
                this.lastPositionReport.clear();
                this.lastPositionReport.addAll(vehiclePositions.get(0));

//                assert (this.inputPositionReport.getTime().shortValue() == this.lastPositionReport.getTime()
//                       .shortValue() + 30);

                this.lastVehiclePosition.set(this.lastPositionReport);

                Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.lastVehiclePosition);
                stoppedCars.remove(vid);
                if (stoppedCars.size() == 0) {
                    this.stoppedCarsPerPosition.remove(this.lastVehiclePosition);
                }
            }

            this.collector.ack(input);
            return;
        }


        List<PositionReport> vehiclePositions = this.lastPositions.get(vid);
        if (vehiclePositions == null) {
            vehiclePositions = new LinkedList<PositionReport>();
            vehiclePositions.add(this.inputPositionReport.copy());
            this.lastPositions.put(vid, vehiclePositions);

            this.collector.ack(input);
            return;
        }

        this.lastPositionReport.clear();
        this.lastPositionReport.addAll(vehiclePositions.get(0));
        //       assert (this.inputPositionReport.getTime() != null);
        //       assert (this.lastPositionReport.getTime() != null);
//        assert (this.inputPositionReport.getTime().shortValue() == this.lastPositionReport.getTime().shortValue() + 30);

        this.vehiclePosition.set(this.inputPositionReport);
        this.lastVehiclePosition.set(this.lastPositionReport);
        if (this.vehiclePosition.equals(this.lastVehiclePosition)) {
            vehiclePositions.add(0, this.inputPositionReport.copy());
            if (vehiclePositions.size() >= 4) {
                LOGGER.trace("Car {} stopped at {} ({})", vid, this.vehiclePosition,
                        new Short(this.inputPositionReport.getMinuteNumber()));
                if (vehiclePositions.size() > 4) {
                    assert (vehiclePositions.size() == 5);
                    vehiclePositions.remove(4);
                }
                Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.vehiclePosition);
                if (stoppedCars == null) {
                    stoppedCars = new HashSet<Integer>();
                    stoppedCars.add(vid);
                    this.stoppedCarsPerPosition.put(this.vehiclePosition.copy(), stoppedCars);
                } else {
                    stoppedCars.add(vid);

                    if (stoppedCars.size() > 1) {
                        this.collector.emit(
                                TopologyControl.ACCIDENTS_STREAM_ID,
                                new AccidentTuple(inputPositionReport, new Short(minute),
                                        this.inputPositionReport.getXWay(), this.inputPositionReport.getSegment(),
                                        this.inputPositionReport.getDirection()));
                    }
                }

            }
        } else {
            if (vehiclePositions.size() == 4) {
                Set<Integer> stoppedCars = this.stoppedCarsPerPosition.get(this.lastVehiclePosition);
                stoppedCars.remove(vid);
                if (stoppedCars.size() == 0) {
                    this.stoppedCarsPerPosition.remove(this.lastVehiclePosition);
                }
            }
            vehiclePositions.clear();
            vehiclePositions.add(this.inputPositionReport.copy());
        }

        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(TopologyControl.ACCIDENTS_STREAM_ID, AccidentTuple.getSchema());
    }

}
