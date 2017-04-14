package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.util.event.AccidentEvent;
import flink.applications.util.event.LAVEvent;
import flink.applications.util.event.NOVEvent;
import flink.applications.util.event.PositionReportEvent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import static flink.applications.constants.LinearRoadConstants.*;

/**
 * Created by szhang026 on 20/2/2016.
 */
public class TollBolt extends flink.applications.bolt.base.AbstractBolt {
    LinkedList cars_list;
    HashMap<Integer, Car> carMap;
    HashMap<Byte, AccNovLavTuple> segments;
    byte NUM_SEG_DOWNSTREAM = 5; //Number of segments downstream to check whether an accident has happened or not
    int BASE_TOLL = 2; //This is a predefined constant (mentioned in Richard's thesis)
    private int count;
    private LinkedList<PositionReportEvent> posEvtList;

    public TollBolt() {
        carMap = new HashMap<Integer, Car>();
        cars_list = new LinkedList();
        segments = new HashMap<Byte, AccNovLavTuple>();
        posEvtList = new LinkedList<PositionReportEvent>();

        setFields("toll_event", new Fields("vid",
                "mile",
                "toll"));
    }

    @Override
    public void execute(Tuple tuple) {
        long typeField = tuple.getLong(0);

        if (NOV_EVENT_TYPE == typeField) {
            //_collector.emit("nov_event", new Values(Constants.NOV_EVENT_TYPE, ((int)Math.floor(currentSecond/60)), mile, numVehicles));
            try {
                NOVEvent obj2 = new NOVEvent(tuple.getInteger(1), tuple.getByte(2), tuple.getInteger(3));
                novEventOccurred(obj2);
            } catch (NumberFormatException e) {
                System.out.println("Not Number Format Exception for tuple : " + tuple);
            }
        } else if (LAV_EVENT_TYPE == typeField) {
            //_collector.emit("lav_event", new Values(Constants.LAV_EVENT_TYPE, mile, lav, i));
            LAVEvent obj = new LAVEvent(tuple.getByte(1), tuple.getFloat(2), tuple.getByte(3));
            lavEventOcurred(obj);
        } else if (ACCIDENT_EVENT_TYPE == typeField) {
            //_collector.emit("accident_event", new Values(a_event.vid1, a_event.vid2, a_event.xway, a_event.mile, a_event.dir));
            AccidentEvent accidentEvent = new AccidentEvent();
            accidentEvent.vid1 = tuple.getInteger(1);
            accidentEvent.vid2 = tuple.getInteger(2);
            accidentEvent.xway = tuple.getByte(3);
            accidentEvent.mile = tuple.getByte(4);
            accidentEvent.dir = tuple.getByte(5);

            accidentEventOccurred(accidentEvent);
        } else {
            PositionReportEvent posEvt = new PositionReportEvent();
            //new Values(time, vid, spd, xway, lane, dir, mile)
            posEvt.time = tuple.getLong(0);
            posEvt.vid = tuple.getInteger(1);
            posEvt.speed = tuple.getByte(2);
            posEvt.xWay = tuple.getByte(3);
            posEvt.lane = tuple.getByte(4);
            posEvt.dir = tuple.getByte(5);
            posEvt.mile = tuple.getByte(6);
            posEvt.offset = tuple.getShort(7);

            process(posEvt);
        }

        collector.ack(tuple);
    }

    public void accidentEventOccurred(AccidentEvent accEvent) {
        System.out.println("Accident Occurred :" + accEvent.toString());
        boolean flg = false;

        synchronized (this) {
            flg = segments.containsKey(accEvent.mile);
        }

        if (!flg) {
            AccNovLavTuple obj = new AccNovLavTuple();
            obj.isAcc = true;
            synchronized (this) {
                segments.put(accEvent.mile, obj);
            }
        } else {
            synchronized (this) {
                AccNovLavTuple obj = segments.get(accEvent.mile);
                obj.isAcc = true;
                segments.put(accEvent.mile, obj);
            }
        }
    }
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream("toll_event", new Fields("vid",
//                "mile",
//                "toll"));
//    }

    public void novEventOccurred(NOVEvent novEvent) {
        boolean flg = false;

        flg = segments.containsKey(novEvent.segment);

        if (!flg) {
            AccNovLavTuple obj = new AccNovLavTuple();
            obj.nov = novEvent.nov;
            segments.put(novEvent.segment, obj);
        } else {
            AccNovLavTuple obj = segments.get(novEvent.segment);
            obj.nov = novEvent.nov;

            segments.put(novEvent.segment, obj);
        }
    }

    public void lavEventOcurred(LAVEvent lavEvent) {
        boolean flg = false;

        flg = segments.containsKey(lavEvent.segment);

        if (!flg) {
            AccNovLavTuple obj = new AccNovLavTuple();
            obj.lav = lavEvent.lav;
            segments.put(lavEvent.segment, obj);
        } else {
            AccNovLavTuple obj = segments.get(lavEvent.segment);
            obj.lav = lavEvent.lav;
            segments.put(lavEvent.segment, obj);
        }
    }

    public void process(PositionReportEvent evt) {
        int len = 0;
        Iterator<Car> itr = cars_list.iterator();

        if (!carMap.containsKey(evt.vid)) {
            Car c = new Car();
            c.carid = evt.vid;
            c.mile = evt.mile;
            carMap.put(evt.vid, c);
        } else {
            Car c = carMap.get(evt.vid);

            if (c.mile != evt.mile) { //Car is entering a new mile/new segment
                c.mile = evt.mile;
                carMap.put(evt.vid, c);

                if ((evt.lane != 0) && (evt.lane != 7)) { //This is to make sure that the car is not on an exit ramp
                    AccNovLavTuple obj = null;

                    obj = segments.get(evt.mile);

                    if (obj != null) {
                        if (isInAccidentZone(evt)) {
                            System.out.println("Its In AccidentZone");
                        }

                        if (((obj.nov < 50) || (obj.lav > 40)) || isInAccidentZone(evt)) {
                            collector.emit("toll_event", new Values(evt.vid, evt.mile, 0));

                        } else {

                            if (segments.containsKey(evt.mile)) {
                                AccNovLavTuple tuple = null;

                                synchronized (this) {
                                    tuple = segments.get(evt.mile);
                                }

                                collector.emit("toll_event", new Values(evt.vid, evt.mile, BASE_TOLL * (tuple.nov - 50) * (tuple.nov - 50)));
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean isInAccidentZone(PositionReportEvent evt) {
        byte mile = evt.mile;
        byte checkMile = (byte) (mile + NUM_SEG_DOWNSTREAM);

        while (mile < checkMile) {
            if (segments.containsKey(mile)) {
                AccNovLavTuple obj = segments.get(mile);

                if (Math.abs((evt.time - obj.time)) > 20) {
                    obj.isAcc = false;
                    mile++;
                    continue; //May be we remove the notification for a particular mile down the xway. But another mile still might have accident. Therefore, we cannot break here.
                }

                if (obj.isAcc) {
                    return true;
                }
            }
            mile++;
        }

        return false;
    }

    private class AccNovLavTuple {
        public boolean isAcc;
        public int nov;
        public float lav;
        public long time;
    }

    private class Car {
        long time;
        int carid;
        byte speed, mile, xway, lane, dir, offset;
        boolean notified;

        public Car() {
            this.time = -1;
            this.carid = 0;
            this.speed = 0;
            this.xway = -1;
            this.lane = -1;
            this.dir = -1;
        }

        public Car(long time, int carid, byte speed, byte xway0, byte lane0, byte dir0, byte mile) {
            this.time = time;
            this.carid = carid;
            this.speed = speed;
            this.xway = xway0;
            this.lane = lane0;
            this.dir = dir0;
            this.mile = mile;
        }
    }
}
