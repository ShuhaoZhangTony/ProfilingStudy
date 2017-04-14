package flink.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import flink.applications.util.stream.StreamValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ClickStreamParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

    private final Gson gson = new Gson();

    @Override
    public List<StreamValues> parse(String input) {
        StreamValues values = null;

        try {
            ClickStream clickstream = gson.fromJson(input, ClickStream.class);
            values = new StreamValues(clickstream.ip, clickstream.url, clickstream.clientKey);
        } catch (JsonSyntaxException ex) {
            LOG.error("Error parsing JSON encoded clickstream: " + input, ex);
        }

        return ImmutableList.of(values);
    }

    @Override
    public List<StreamValues> parse(String[] input) {
        return null;
    }

    private static class ClickStream {
        public String ip;
        public String url;
        public String clientKey;
    }
}
