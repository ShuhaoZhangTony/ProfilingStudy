package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.util.stream.StreamValues;

import java.util.LinkedList;
import java.util.List;


public class lrParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(lrfParser.class);

    @Override
    public List<StreamValues> parse(String line) {
        if (StringUtils.isBlank(line))
            return null;

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
        StreamValues sv = null;
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
                sv = new StreamValues(time, vid, spd, xway, lane, dir, mile, ofst);
                sv.setStreamId("position_report");
                break;
            case 2:
                time = Long.parseLong(fields[1]);
                vid = Integer.parseInt(fields[2]);
                qid = Integer.parseInt(fields[9]);
                //This is an Account Balance report (Type=2, Time, VID, QID)
                // collector.emit("accbal_report", new Values(time, vid, qid),cnt++);
                sv = new StreamValues(time, vid, qid);
                sv.setStreamId("accbal_report");

                break;
            case 3:
                time = Long.parseLong(fields[1]);
                vid = Integer.parseInt(fields[2]);
                xway = Byte.parseByte(fields[4]);
                qid = Integer.parseInt(fields[9]);
                day = Integer.parseInt(fields[14]);
                //This is an Expenditure report (Type=3, Time, VID, XWay, QID, Day)
                //collector.emit("daily_exp", new Values(time, vid, xway, qid, day),cnt++);
                sv = new StreamValues(time, vid, xway, qid, day);
                sv.setStreamId("daily_exp");
                break;
            case 4:
                //This is a travel time report (Types=4, Time, VID, XWay, QID, Sinit, Send, DOW, TOD)
                return null;
            case 5:
                System.out.println("Travel time query was issued : " + line);
                return null;
        }


        return ImmutableList.of(sv);
    }

    @Override
    public List<StreamValues> parse(String[] input) {
        List<String> srt = new LinkedList<>();

        for (int i = 0; i < input.length; i++) {
            if (input[i] != null)
                srt.add(input[i]);
        }
        StreamValues rt = new StreamValues(srt);
        return ImmutableList.of(rt);
    }
}