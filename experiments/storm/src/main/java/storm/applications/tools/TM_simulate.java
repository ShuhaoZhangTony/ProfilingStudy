package storm.applications.tools;

/**
 * Created by szhang026 on 5/30/2016.
 * A simmulation of TM > MapMather
 */

import com.google.common.collect.ImmutableList;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.MapMatchingBolt;
import storm.applications.constants.BaseConstants;
import storm.applications.constants.TrafficMonitoringConstants;
import storm.applications.model.gis.GPSRecord;
import storm.applications.model.gis.Road;
import storm.applications.model.gis.RoadGridList;
import storm.applications.spout.parser.Parser;
import storm.applications.util.OsUtils;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.config.Configuration;
import storm.applications.util.data.Tuple;
import storm.applications.util.stream.StreamValues;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.sql.SQLException;
import java.util.*;

import static storm.applications.StormRunner.loadProperties;

public class TM_simulate {
    class Control {
        public volatile int index_e = 0;
        public volatile int received = 0;
        public volatile long start, end;
        public volatile int started_thread = 0;
        public volatile int total_thread = 0;
        public volatile boolean run_start = false;
    }

    final Control control = new Control();

    private static final int ID_FIELD = 0;
    private static final int NID_FIELD = 1;
    private static final int DATE_FIELD = 2;
    private static final int LAT_FIELD = 3;
    private static final int LON_FIELD = 4;
    private static final int SPEED_FIELD = 5;
    private static final int DIR_FIELD = 6;
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final Logger LOG = LoggerFactory.getLogger(TM_simulate.class);

    class Worker implements Runnable {
        private final String CFG_PATH;
        private final String metric_path;
        protected Parser parser;
        protected String[] array;


        protected Configuration config;
        public boolean running = false;
        private RoadGridList sectors;
        protected int end_index = 0;//32M
        private Map<Integer, Road> roads;

        public Worker() throws IOException {
            roads = new HashMap<>();
            if (OsUtils.isWindows()) {
                CFG_PATH = "src\\main\\resources\\config\\%s.properties";
                metric_path = "metric_output\\test";
            } else {
                CFG_PATH = "/home/storm/storm-app/src/main/resources/config/%s.properties";
                metric_path = "/home/storm/metric_output/test";
            }
            String cfg = String.format(CFG_PATH, "traffic-monitoring");
            //LOG.info("1. Loaded default configuration file {}", cfg);
            Properties p = loadProperties(cfg, true);
            config = Configuration.fromProperties(p);

            String shapeFile;
            if (OsUtils.isWindows())
                shapeFile = "C://Users//szhang026//Documents//Profile-experiments//TestingData//data//app//tm//beijing//roads.shp";
            else
                shapeFile = "/media/tony/ProfilingData/TestingData/data/app/tm/beijing/roads.shp";

            try {
                sectors = new RoadGridList(config, shapeFile);
            } catch (SQLException | IOException ex) {
                LOG.error("Error while loading shape file", ex);
                throw new RuntimeException("Error while loading shape file");
            }
            String path;
            if (OsUtils.isWindows())
                path = "C://Users//szhang026//Documents//Profile-experiments//TestingData//data//app//tm//taxi-traces.csv";
            else {
                path = "/media/tony/ProfilingData/TestingData/data/app/tm/taxi-traces.csv";
            }

            String parserClass = "storm.applications.spout.parser.BeijingTaxiTraceParser";
            parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
            parser.initialize(config);
            List<String> str_l = new LinkedList<String>();
            try {
                openFile(path, str_l);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            Thread thread = new Thread(this);
            thread.start();
        }

        public void openFile(String fileName, List<String> str_l) throws FileNotFoundException {
            Scanner scanner = new Scanner(new File(fileName), "UTF-8");
            //int count =100;
            while (scanner.hasNextLine()) {
                //count--;
                str_l.add(scanner.nextLine()); //normal..
            }
            scanner.close();
            array = str_l.toArray(new String[str_l.size()]);
            end_index = array.length;
//        index_e = 0;
//        System.out.println("spout end:" + end_index);
        }


        public List<StreamValues> parse(String input) {
            String[] fields = input.split(",");

            if (fields.length != 7)
                return null;

            try {
                String carId = fields[ID_FIELD];
                DateTime date = formatter.parseDateTime(fields[DATE_FIELD]);
                boolean occ = true;
                double lat = Double.parseDouble(fields[LAT_FIELD]);
                double lon = Double.parseDouble(fields[LON_FIELD]);
                int speed = ((Double) Double.parseDouble(fields[SPEED_FIELD])).intValue();
                int bearing = Integer.parseInt(fields[DIR_FIELD]);

                int msgId = String.format("%s:%s", carId, date.toString()).hashCode();

                StreamValues values = new StreamValues(carId, date, occ, speed, bearing, lat, lon);
                values.setMessageId(msgId);

                return ImmutableList.of(values);
            } catch (NumberFormatException ex) {
                LOG.warn("Error parsing numeric value", ex);
            } catch (IllegalArgumentException ex) {
                LOG.warn("Error parsing date/time value", ex);
            }

            return null;
        }

        @Override
        public void run() {
            this.running = true;
            ++control.started_thread;
            LOG.info("This is currently running on a separate thread, " +
                    "the id is: " + Thread.currentThread().getId() + "current started thread number:" + control.started_thread);
            if (control.started_thread == control.total_thread) {
                LOG.info("Running start");
                control.start = System.nanoTime();
                control.run_start = true;
            }

            String value = null;

            while (0 <= control.index_e && control.index_e < end_index) {
                if (control.run_start) {

                    value = array[control.index_e++ % array.length];
                    List<StreamValues> tuples = null;
                    tuples = parser.parse(value);
                    for (StreamValues tvalues : tuples) {

                /* spout.setFields(new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE));*/
                        List<Object> input = tvalues;
                        //  input.toArray();
                        // public TupleImpl(GeneralTopologyContext context, List<Object> values, int taskId, String streamId, MessageId id) {
                        try {
                            int speed = (int) input.get(3);
                            int bearing = (int) input.get(4);
                            double latitude = (double) input.get(5);
                            double longitude = (double) input.get(6);

                            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);

                            int roadID = sectors.fetchRoadID(record);

                            if (roadID != -1) {
                                List<Object> values = input;
                                values.add(roadID);

                                speedCal(values);
                            }
                        } catch (SQLException ex) {
                            LOG.error("Unable to fetch road ID", ex);
                        }
                    }
                }
            }
            this.running = false;
        }

        private void speedCal(List<Object> input) {
            int roadID = (int) input.get(7);
            int speed = (int) input.get(3);

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
            if (control.received++ == 15600) {
                //System.out.println(averageSpeed + "" + count);
                LOG.info("Running finished");
                control.end = System.nanoTime();
                LOG.info("finished execution in:" + String.valueOf((control.end - control.start) / 1000000000));
            }
        }
    }


    public void start_test(int no_worker) throws IOException, InterruptedException {
        control.total_thread = no_worker;
        List<Worker> workers = new ArrayList<Worker>();
//        System.out.println("This is currently running on the main thread, " +
//                "the id is: " + Thread.currentThread().getId());


        // start 5 workers
        for (int i = 0; i < no_worker; i++) {
            workers.add(new Worker());
        }

        // We must force the main thread to wait for all the workers
        //  to finish their work before we check to see how long it
        //  took to complete
        for (Worker worker : workers) {
            while (worker.running) {
                Thread.sleep(100);
            }
        }

        //long difference = end.getTime() - start.getTime();

        //System.out.println("This whole process took: " + difference / 1000 + " seconds.");
        try {
            FileWriter fw;
            BufferedWriter writer = null;
            try {
                fw = new FileWriter(new File("/home/storm/" + OsUtils.OS_wrapper("sink.txt")));

                writer = new BufferedWriter(fw);
                //writer.write("Received first element\n");
                //writer.flush();

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
//        		//System.out.println("Finished execution in:"+((end-start)/1000.0)/1000000.0+" seconds"+"index_e"+index_e);
            //writer.write(start_true+"\n");

//    			for(int i=0;i<recorder.size();i++){
//    				writer.write(recorder.get(i)+"\n");
//    			}
            LOG.info("finished execution in:" + String.valueOf((control.end - control.start) / 1000000000));

            //writer.write(((end-start)/1000.0)/1000000.0+"\t"+index_e+"\n");
            //writer.write(((end-start_true)/1000.0)/1000000.0+"\t"+index_e+"\n");
            writer.flush();
            writer.close();
//                try {
//					TimeUnit.SECONDS.sleep(5);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

    public static void main(String[] arg) {
        TM_simulate my_Simu = new TM_simulate();
        try {
            int no_w;
            if (arg.length > 0)
                no_w = Integer.parseInt(arg[0]);
            else
                no_w = 5;
            my_Simu.start_test(no_w);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


