package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.util.event.AccidentEvent;

import java.util.LinkedList;

import static flink.applications.constants.LinearRoadConstants.ACCIDENT_EVENT_TYPE;

/**
 * Created by szhang026 on 20/2/2016.
 */
public class AccidentDetectionBolt extends flink.applications.bolt.base.AbstractBolt {
    public static LinkedList smashed_cars = new LinkedList();
    public static LinkedList stopped_cars = new LinkedList();
    public static LinkedList accidents = new LinkedList();

    public AccidentDetectionBolt() {

        setFields("accident_event", new Fields("type",
                "vid1",
                "vid2",
                "xway",
                "mile",
                "dir"));
    }

    @Override
    public void execute(Tuple tuple) {
        Car c = new Car(tuple.getLong(0),
                tuple.getInteger(1),
                tuple.getByte(2),
                tuple.getByte(3),
                tuple.getByte(6),
                tuple.getByte(4),
                tuple.getByte(5));

        detect(c);
        collector.ack(tuple);
    }

    //    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        //We declare the streams that we are emitting from Accident Detect bolt.
//        //nov_tuples
//        declarer.declareStream("accident_event", new Fields("type",
//                "vid1",
//                "vid2",
//                "xway",
//                "mile",
//                "dir"));
//    }
    public void detect(Car c) {
        if (c.speed > 0) {
            remove_from_smashed_cars(c);
            remove_from_stopped_cars(c);
        } else if (c.speed == 0) {
            if (is_smashed_car(c) == false) {
                if (is_stopped_car(c) == true) {
                    renew_stopped_car(c);
                } else {
                    stopped_cars.add(c);
                }

                int flag = 0;
                for (int i = 0; i < stopped_cars.size() - 1; i++) {
                    Car t_car = (Car) stopped_cars.get(i);
                    if ((t_car.carid != c.carid) && (!t_car.notified) && ((c.time - t_car.time) <= 120) && (t_car.posReportID >= 3) &&
                            ((c.xway0 == t_car.xway0 && c.mile0 == t_car.mile0 && c.lane0 == t_car.lane0 && c.offset0 == t_car.offset0 && c.dir0 == t_car.dir0) &&
                                    (c.xway1 == t_car.xway1 && c.mile1 == t_car.mile1 && c.lane1 == t_car.lane1 && c.offset1 == t_car.offset1 && c.dir1 == t_car.dir1) &&
                                    (c.xway2 == t_car.xway2 && c.mile2 == t_car.mile2 && c.lane2 == t_car.lane2 && c.offset2 == t_car.offset2 && c.dir2 == t_car.dir2) &&
                                    (c.xway3 == t_car.xway3 && c.mile3 == t_car.mile3 && c.lane3 == t_car.lane3 && c.offset3 == t_car.offset3 && c.dir3 == t_car.dir3))) {

                        if (flag == 0) {
                            AccidentEvent a_event = new AccidentEvent();
                            a_event.vid1 = c.carid;
                            a_event.vid2 = t_car.carid;
                            a_event.xway = c.xway0;
                            a_event.mile = c.mile0;
                            a_event.dir = c.dir0;
                            a_event.time = t_car.time;

                            collector.emit("accident_event", new Values(ACCIDENT_EVENT_TYPE, a_event.vid1, a_event.vid2, a_event.xway, a_event.mile, a_event.dir));

                            t_car.notified = true;
                            c.notified = true;
                            flag = 1;
                        }
                        //The cars c and t_car have smashed with each other
                        add_smashed_cars(c);
                        add_smashed_cars(t_car);

                        break;
                    }
                }
            }
        }
    }

    public boolean is_smashed_car(Car car) {
        for (int i = 0; i < smashed_cars.size(); i++) {
            Car t_car = (Car) smashed_cars.get(i);

            if (((Car) smashed_cars.get(i)).carid == car.carid) {
                return true;
            }
        }
        return false;
    }

    public void add_smashed_cars(Car c) {
        for (int i = 0; i < smashed_cars.size(); i++) {
            Car t_car = (Car) smashed_cars.get(i);
            if (c.carid == t_car.carid) {
                smashed_cars.remove(i);
            }
        }
        smashed_cars.add(c);
    }

    public boolean is_stopped_car(Car c) {
        for (int i = 0; i < stopped_cars.size(); i++) {
            Car t_car = (Car) stopped_cars.get(i);
            if (c.carid == t_car.carid) {
                return true;
            }
        }
        return false;
    }

    public void remove_from_smashed_cars(Car c) {
        for (int i = 0; i < smashed_cars.size(); i++) {
            Car t_car = (Car) smashed_cars.get(i);
            if (c.carid == t_car.carid) {
                smashed_cars.remove();
            }
        }
    }

    public void remove_from_stopped_cars(Car c) {
        for (int i = 0; i < stopped_cars.size(); i++) {
            Car t_car = (Car) stopped_cars.get(i);
            if (c.carid == t_car.carid) {
                stopped_cars.remove();
            }
        }
    }

    public void renew_stopped_car(Car c) {
        for (int i = 0; i < stopped_cars.size(); i++) {
            Car t_car = (Car) stopped_cars.get(i);
            if (c.carid == t_car.carid) {
                c.xway3 = t_car.xway2;
                c.xway2 = t_car.xway1;
                c.xway1 = t_car.xway0;
                c.mile3 = t_car.mile2;
                c.mile2 = t_car.mile1;
                c.mile1 = t_car.mile0;
                c.lane3 = t_car.lane2;
                c.lane2 = t_car.lane1;
                c.lane1 = t_car.lane0;
                c.offset3 = t_car.offset2;
                c.offset2 = t_car.offset1;
                c.offset1 = t_car.offset0;
                c.dir3 = t_car.dir2;
                c.dir2 = t_car.dir1;
                c.dir1 = t_car.dir0;
                c.notified = t_car.notified;
                c.posReportID = (byte) (t_car.posReportID + 1);

                stopped_cars.remove(i);
                stopped_cars.add(c);

                //Since we already found the car from the list we break at here
                break;
            }
        }
    }

    private class Car {
        long time;
        int carid;
        byte speed, mile0, mile1, mile2, mile3, xway0, xway1, xway2, xway3, lane0, lane1, lane2, lane3, dir0, dir1, dir2, dir3, offset0, offset1, offset2, offset3;
        boolean notified;
        byte posReportID; //This value can range from 0 to 3 (4 position reports)

        public Car() {
            this.time = -1;
            this.carid = 0;
            this.speed = 0;
            this.xway0 = -1;
            this.xway1 = -1;
            this.xway2 = -1;
            this.xway3 = -1;
            this.lane0 = -1;
            this.lane1 = -1;
            this.lane2 = -1;
            this.lane3 = -1;
            this.dir0 = -1;
            this.dir1 = -1;
            this.dir2 = -1;
            this.dir3 = -1;
        }

        public Car(long time, int carid, byte speed, byte xway0, byte lane0, byte dir0, byte mile) {
            this.time = time;
            this.carid = carid;
            this.speed = speed;
            this.xway0 = xway0;
            this.lane0 = lane0;
            this.dir0 = dir0;
            this.mile0 = mile;
        }

        public String toString() {
            return "Car carid=" + this.carid;
        }

        /**
         * We override the default equals method for Car.
         *
         * @param obj2
         * @return
         */
        public boolean equals(Car obj2) {
            if (this.carid == obj2.carid) {
                return true;
            } else {
                return false;
            }
        }
    }
}
