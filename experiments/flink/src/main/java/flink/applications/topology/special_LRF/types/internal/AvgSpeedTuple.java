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
package flink.applications.topology.special_LRF.types.internal;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import flink.applications.topology.special_LRF.TopologyControl;
import flink.applications.topology.special_LRF.types.util.ISegmentIdentifier;
import flink.applications.topology.special_LRF.util.Time;


/**
 * {@link AvgSpeedTuple} represents an intermediate result tuple; the average speed of all vehicle in a segment within a
 * 'minute number' time frame (see {@link Time#getMinute(long)}).<br />
 * <br />
 * It has the following attributes: MINUTE, XWAY, SEGMENT, DIR, AVGS
 * <ul>
 * <li>MINUTE: the 'minute number' of the speed average</li>
 * <li>XWAY: the expressway the vehicle is on</li>
 * <li>SEGMENT: the segment number the vehicle is in</li>
 * <li>DIR: the vehicle's driving direction</li>
 * <li>AVGS: the average speed of the vehicle</li>
 * </ul>
 *
 * @author mjsax
 */
public final class AvgSpeedTuple extends Values implements ISegmentIdentifier {
    /**
     * The index of the MINUTE attribute.
     */
    public final static int MINUTE_IDX = 0;

    // attribute indexes
    /**
     * The index of the XWAY attribute.
     */
    public final static int XWAY_IDX = 1;
    /**
     * The index of the SEGMENT attribute.
     */
    public final static int SEG_IDX = 2;
    /**
     * The index of the DIR attribute.
     */
    public final static int DIR_IDX = 3;
    /**
     * The index of the AVGS attribute.
     */
    public final static int AVGS_IDX = 4;
    private static final long serialVersionUID = 2759896465050962310L;


    public AvgSpeedTuple() {
    }

    /**
     * Instantiates a new {@link AvgSpeedTuple} for the given attributes.
     *
     * @param minute   the 'minute number' of the speed average
     * @param xway     the expressway the vehicle is on
     * @param segment  the segment number the vehicle is in
     * @param diretion the vehicle's driving direction
     * @param avgSpeed the average speed of the vehicle
     */
    public AvgSpeedTuple(Short minute, Integer xway, Short segment, Short diretion, Integer avgSpeed) {
        assert (minute != null);
        assert (xway != null);
        assert (segment != null);
        assert (diretion != null);
        assert (avgSpeed != null);

        super.add(MINUTE_IDX, minute);
        super.add(XWAY_IDX, xway);
        super.add(SEG_IDX, segment);
        super.add(DIR_IDX, diretion);
        super.add(AVGS_IDX, avgSpeed);
    }

    /**
     * Returns the schema of a {@link AvgSpeedTuple}..
     *
     * @return the schema of a {@link AvgSpeedTuple}
     */
    public static Fields getSchema() {
        return new Fields(TopologyControl.MINUTE_FIELD_NAME, TopologyControl.XWAY_FIELD_NAME,
                TopologyControl.SEGMENT_FIELD_NAME, TopologyControl.DIRECTION_FIELD_NAME,
                TopologyControl.AVERAGE_SPEED_FIELD_NAME);
    }

    /**
     * Returns the 'minute number' of this {@link AvgSpeedTuple}.
     *
     * @return the 'minute number' of this tuple
     */
    public final Short getMinuteNumber() {
        return (Short) super.get(MINUTE_IDX);
    }

    /**
     * Returns the expressway ID of this {@link AvgSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Integer getXWay() {
        return (Integer) super.get(XWAY_IDX);
    }

    /**
     * Returns the segment of this {@link AvgSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getSegment() {
        return (Short) super.get(SEG_IDX);
    }

    /**
     * Returns the vehicle's direction of this {@link AvgSpeedTuple}.
     *
     * @return the VID of this tuple
     */
    @Override
    public final Short getDirection() {
        return (Short) super.get(DIR_IDX);
    }

    /**
     * Returns the vehicle's average speed of this {@link AvgSpeedTuple}.
     *
     * @return the average speed of this tuple
     */
    public final Integer getAvgSpeed() {
        return (Integer) super.get(AVGS_IDX);
    }

}
