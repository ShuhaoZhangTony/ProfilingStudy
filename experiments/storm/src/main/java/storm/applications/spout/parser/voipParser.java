package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.stream.StreamValues;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class voipParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(voipParser.class);

    @Override
    public List<StreamValues> parse(String str) {
        if (StringUtils.isBlank(str))
            return null;
        CallDetailRecord cdr = buildcdr(str);
        //cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), cdr
        return ImmutableList.of(new StreamValues(cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), cdr));
    }

    @Override
    public List<StreamValues> parse(String[] input) {
        int batch = input.length;
        LinkedList<CallDetailRecord> cdr = new LinkedList<>();

        for (int i = 0; i < batch; i++) {
            cdr.add(buildcdr(input[i]));
        }
        return ImmutableList.of(new StreamValues(cdr));
    }


    private CallDetailRecord buildcdr(String str) {
        String[] psb = str.substring(1, str.length() - 1)
                .split(",");

        CallDetailRecord cdr = new CallDetailRecord();

        cdr.setCallingNumber(psb[0].replaceAll(" ", ""));
        cdr.setCalledNumber(psb[1].replaceAll(" ", ""));

        String dateTime = psb[2].replaceAll(" ", "");

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        DateTime dt = DateTime.parse(dateTime, formatter);

        cdr.setAnswerTime(dt);
        cdr.setCallDuration(Integer.parseInt(psb[6].split("=")[1].replaceAll(" ", "")));
        cdr.setCallEstablished(Boolean.parseBoolean(psb[7].split("=")[1].replaceAll("}", "")));
        return cdr;
    }
}