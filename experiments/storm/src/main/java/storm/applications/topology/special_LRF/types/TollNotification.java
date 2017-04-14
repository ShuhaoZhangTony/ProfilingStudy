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
package storm.applications.topology.special_LRF.types;

import org.apache.storm.tuple.Fields;
import storm.applications.topology.special_LRF.TopologyControl;


/**
 * An {@link TollNotification} represent an toll information that must be sent to vehicles entering a new segment.<br />
 * <br />
 * Toll notifications do have the following attributes (VID index not as specified in LRB for better code re-usage):
 * TYPE=0, TIME, EMIT, VID, SPEED, TOLL
 * <ul>
 * <li>TYPE: the tuple type ID</li>
 * <li>TIME: the timestamp of the {@link PositionReport} that triggered the toll notification to be generated (in LRB
 * seconds)</li>
 * <li>EMIT: 'the time the notification is emitted' (in LRB seconds)</li>
 * <li>VID: the ID of the vehicle that is notified of the toll</li>
 * <li>SPEED: 'the 5-minute average speed in the segment</li>
 * <li>TOLL: 'the calculated toll'</li>
 * </ul>
 *
 * @author mjsax
 */
public class TollNotification extends AbstractOutputTuple {
    /**
     * The index of the VID attribute.
     */
    public final static int VID_IDX = 3;

    // attribute indexes
    /**
     * The index of the speed attribute.
     */
    public final static int SPEED_IDX = 4;
    /**
     * The index of the toll attribute.
     */
    public final static int TOLL_IDX = 5;
    /**
     * The index of the POS attribute.
     */
    public final static int POS_IDX = 6;
    private static final long serialVersionUID = -6980996098837847843L;

    public TollNotification() {
        super();
    }

    /**
     * Instantiates a new accident notification for the given attributes.
     *
     * @param time  the time or the position record triggering this notification
     * @param emit  the emit time of the notification
     * @param vid   the ID of the vehicle that is notified of the toll
     * @param speed the 5-minute average speed in the segment
     * @param toll  the calculated toll
     */
    public TollNotification(Short time, Short emit, Integer vid, Integer speed, Integer toll) {
        super(AbstractLRBTuple.TOLL_NOTIFICATION, time, emit);

        assert (vid != null);
        assert (speed != null);
        assert (toll != null);

        super.add(VID_IDX, vid);
        super.add(SPEED_IDX, speed);
        super.add(TOLL_IDX, toll);

        assert (super.size() == 6);
    }

    /**
     * Instantiates a new accident notification for the given attributes.
     *
     * @param time  the time or the position record triggering this notification
     * @param emit  the emit time of the notification
     * @param vid   the ID of the vehicle that is notified of the toll
     * @param speed the 5-minute average speed in the segment
     * @param toll  the calculated toll
     */
    public TollNotification(Short time, Short emit, Integer vid, Integer speed, Integer toll, PositionReport pos) {
        super(AbstractLRBTuple.TOLL_NOTIFICATION, time, emit);

        assert (vid != null);
        assert (speed != null);
        assert (toll != null);

        super.add(VID_IDX, vid);
        super.add(SPEED_IDX, speed);
        super.add(TOLL_IDX, toll);
        super.add(pos);
        assert (super.size() == 7);
    }

    /**
     * Returns the schema of an {@link TollNotification}.
     *
     * @return the schema of an {@link TollNotification}
     */
    public static Fields getSchema() {
        return new Fields(TopologyControl.TYPE_FIELD_NAME, TopologyControl.TIME_FIELD_NAME,
                TopologyControl.EMIT_FIELD_NAME, TopologyControl.VEHICLE_ID_FIELD_NAME, TopologyControl.SPEED_FIELD_NAME,
                TopologyControl.TOLL_FIELD_NAME, TopologyControl.POS_REPORT_FIELD_NAME);
    }

    /**
     * Returns the vehicle ID of this {@link TollNotification}.
     *
     * @return the VID of this tuple
     */
    public final Integer getVid() {
        return (Integer) super.get(VID_IDX);
    }

    /**
     * Returns the 5-minute average speed of the segment of this {@link TollNotification}.
     *
     * @return the speed of this tuple
     */
    public final Integer getSpeed() {
        return (Integer) super.get(SPEED_IDX);
    }

    /**
     * Returns the toll of this {@link TollNotification}.
     *
     * @return the toll of this tuple
     */
    public final Integer getToll() {
        return (Integer) super.get(TOLL_IDX);
    }

    /**
     * Returns the toll of this {@link TollNotification}.
     *
     * @return the toll of this tuple
     */
    public final PositionReport getPos() {
        return (PositionReport) super.get(POS_IDX);
    }

    /**
     * Compares the specified object with this {@link TollNotification} object for equality. Returns true if and only if
     * the specified object is also a {@link TollNotification} and their TIME, VID, SPEED, and TOLL attributes are
     * equals. The EMIT attribute is not considered. Furthermore, TYPE is known to be equal if the specified object is
     * of type {@link TollNotification}.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        TollNotification other = (TollNotification) obj;
        assert (this.getType().equals(other.getType()));

        if (this.getTime() == null) {
            if (other.getTime() != null) {
                return false;
            }
        } else if (!this.getTime().equals(other.getTime())) {
            return false;
        }

        if (this.getVid() == null) {
            if (other.getVid() != null) {
                return false;
            }
        } else if (!this.getVid().equals(other.getVid())) {
            return false;
        }

        if (this.getSpeed() == null) {
            if (other.getSpeed() != null) {
                return false;
            }
        } else if (!this.getSpeed().equals(other.getSpeed())) {
            return false;
        }

        if (this.getToll() == null) {
            if (other.getToll() != null) {
                return false;
            }
        } else if (!this.getToll().equals(other.getToll())) {
            return false;
        }

        return true;
    }
}
