package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.util.stream.StreamValues;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class msParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(msParser.class);

    @Override
    public List<StreamValues> parse(String str) {
        if (StringUtils.isBlank(str))
            return null;

        return ImmutableList.of(new StreamValues(str));
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