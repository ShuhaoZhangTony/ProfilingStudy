package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.util.stream.StreamValues;

import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class JsonParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
    private static final JSONParser jsonParser = new JSONParser();

    @Override
    public List<StreamValues> parse(String input) {
        input = input.trim();

        if (input.isEmpty() || (!input.startsWith("{") && !input.startsWith("[")))
            return null;

        try {
            JSONObject json = (JSONObject) jsonParser.parse(input);
            return ImmutableList.of(new StreamValues(json));
        } catch (ParseException e) {
            LOG.error(String.format("Error parsing JSON object: %s", input), e);
        }

        return null;
    }

    @Override
    public List<StreamValues> parse(String[] input) {
        return null;
    }
}