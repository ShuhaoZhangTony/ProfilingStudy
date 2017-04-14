package flink.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import flink.applications.util.data.DateUtils;
import flink.applications.util.stream.StreamValues;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CommonLogParser extends Parser {
    public static final String IP = "ip";
    public static final String TIMESTAMP = "timestamp";
    public static final String REQUEST = "request";
    public static final String RESPONSE = "response";
    public static final String BYTE_SIZE = "byte_size";
    private static final Logger LOG = LoggerFactory.getLogger(CommonLogParser.class);
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
    private static final int NUM_FIELDS = 8;

    public static Map<String, Object> parseLine(String logLine) {
        Map<String, Object> entry = new HashMap<>();
        String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)";


        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logLine);

        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            return null;
        }

        entry.put(IP, matcher.group(1));
        entry.put(TIMESTAMP, dtFormatter.parseDateTime(matcher.group(4)).toDate());
        entry.put(REQUEST, matcher.group(5));
        entry.put(RESPONSE, Integer.parseInt(matcher.group(6)));

        if (matcher.group(7).equals("-"))
            entry.put(BYTE_SIZE, 0);
        else
            entry.put(BYTE_SIZE, Integer.parseInt(matcher.group(7)));

        return entry;
    }

    @Override
    public List<StreamValues> parse(String str) {
        Map<String, Object> entry = parseLine(str);

        if (entry == null) {
            LOG.warn("Unable to parse log: {}", str);
            return null;
        }

        long minute = DateUtils.getMinuteForTime((Date) entry.get(TIMESTAMP));
        int msgId = String.format("%s:%s", entry.get(IP), entry.get(TIMESTAMP)).hashCode();

        StreamValues values = new StreamValues(entry.get(IP), entry.get(TIMESTAMP),
                minute, entry.get(REQUEST), entry.get(RESPONSE), entry.get(BYTE_SIZE));
        values.setMessageId(msgId);

        return ImmutableList.of(values);
    }

    @Override
    public List<StreamValues> parse(String[] input) {
        Map<String, Object>[] entry = new Map[input.length];
        String[] OIP = new String[input.length];
        Object[] OTIMESTAMP = new Object[input.length];
        long[] Ominute = new long[input.length];
        Object[] OREQUEST = new Object[input.length];
        int[] ORESPONSE = new int[input.length];
        Object[] OBYTE_SIZE = new Object[input.length];

        for (int i = 0; i < input.length; i++) {
            entry[i] = parseLine(input[i]);
            OIP[i] = (String) entry[i].get(IP);
            OTIMESTAMP[i] = entry[i].get(TIMESTAMP);
            Ominute[i] = DateUtils.getMinuteForTime((Date) entry[i].get(TIMESTAMP));
            OREQUEST[i] = entry[i].get(REQUEST);
            ORESPONSE[i] = (int) entry[i].get(RESPONSE);
            OBYTE_SIZE[i] = entry[i].get(BYTE_SIZE);
        }
        StreamValues values = new StreamValues(OIP, OTIMESTAMP,
                Ominute, OREQUEST, ORESPONSE, OBYTE_SIZE);
        return ImmutableList.of(values);

    }
}
