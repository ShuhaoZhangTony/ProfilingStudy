package storm.applications.spout;

import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import static storm.applications.constants.LinearRoadConstants.LINEAR_CAR_DATA_POINTS;

/**
 * Created by szhang026 on 20/2/2016.
 */
public class InputEventInjectorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);
    String carDataFile;
    long cnt = 0, nhist_cnt = 0;
    boolean completedLoadingHistory = false; //This flag indicates whether we have completed loading history data or not.

    @Override
    protected void initialize() {
        carDataFile = config.getString(LINEAR_CAR_DATA_POINTS);
    }
//    @Override
//    protected void initialize() {
////        Properties properties = new Properties();
////        InputStream propertiesIS = InputEventInjectorSpout.class.getClassLoader().getResourceAsStream(CONFIG_FILENAME);
////        if (propertiesIS == null) {
////            throw new RuntimeException("Properties file '" + CONFIG_FILENAME + "' not found in classpath");
////        }
////        try {
////            properties.load(propertiesIS);
////        } catch (IOException ec) {
////            ec.printStackTrace();
////        }
//
//
//    }

//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        //Here we declare the set of streams that are emitted by this Spout.
//
//        //_collector.emit("position_report", new Values(time, vid, spd, xway, lane, dir, mile));
//
//        //Position report Stream (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
//        declarer.declareStream("position_report", new Fields(
//                "secfromstart",
//                "vid",
//                "speed",
//                "xway",
//                "lane",
//                "dir",
//                "mile",
//                "ofst"));
//        //Account Balance Report Stream (Type=2, Time, VID, QID)
//        declarer.declareStream("accbal_report", new Fields(
//                "secfromstart",
//                "vid",
//                "qid"));
//        //Expenditure report (Type=3, Time, VID, XWay, QID, Day)
//        declarer.declareStream("daily_exp", new Fields(
//                "secfromstart",
//                "vid",
//                "xway",
//                "qid",
//                "day"));
//    }

    @Override
    public void nextTuple() {
        try {
            BufferedReader in = new BufferedReader(new FileReader(carDataFile));
            String line;
            while ((line = in.readLine()) != null) {
                Long time = -1l;
                Integer vid = -1;
                Integer qid = -1;
                Byte spd = -1;
                Byte xway = -1;
                Byte mile = -1;
                Short ofst = -1;
                Byte lane = -1;
                Byte dir = -1;
                Integer day = -1;

                line = line.substring(2, line.length() - 1);

                String[] fields = line.split(" ");
                byte typeField = Byte.parseByte(fields[0]);

                //In the case of Storm it seems that we need not to send the type of the tuple through the network. It is because
                //the event itself has some associated type. This seems to be an advantage of Storm compared to other message passing based solutions.
                switch (typeField) {
                    case 0:
                        //Need to calculate the offset value
                        String offset = "" + ((short) (Integer.parseInt(fields[8]) - (Integer.parseInt(fields[7]) * 5280)));
                        //This is a position report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
                        time = Long.parseLong(fields[1]);
                        vid = Integer.parseInt(fields[2]);
                        spd = Byte.parseByte(fields[3]);
                        xway = Byte.parseByte(fields[4]);
                        lane = Byte.parseByte(fields[5]);
                        dir = Byte.parseByte(fields[6]);
                        mile = Byte.parseByte(fields[7]);
                        ofst = Short.parseShort(offset);
                        collector.emit("position_report", new Values(time, vid, spd, xway, lane, dir, mile, ofst), cnt++);
                        break;
                    case 2:
                        time = Long.parseLong(fields[1]);
                        vid = Integer.parseInt(fields[2]);
                        qid = Integer.parseInt(fields[9]);
                        //This is an Account Balance report (Type=2, Time, VID, QID)
                        //collector.emit("accbal_report", new Values(time, vid, qid),cnt++);
                        break;
                    case 3:
                        time = Long.parseLong(fields[1]);
                        vid = Integer.parseInt(fields[2]);
                        xway = Byte.parseByte(fields[4]);
                        qid = Integer.parseInt(fields[9]);
                        day = Integer.parseInt(fields[14]);
                        //This is an Expenditure report (Type=3, Time, VID, XWay, QID, Day)
                        collector.emit("daily_exp", new Values(time, vid, xway, qid, day), cnt++);
                        break;
                    case 4:
                        //This is a travel time report (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
                        break;
                    case 5:
                        System.out.println("Travel time query was issued : " + line);
                        break;
                }
            }
            System.out.println("Done emitting the input tuples..." + cnt);
        } catch (IOException ec) {
            ec.printStackTrace();
        }
    }
}
