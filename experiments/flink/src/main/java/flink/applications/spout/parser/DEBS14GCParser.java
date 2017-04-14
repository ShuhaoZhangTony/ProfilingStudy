package flink.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import flink.applications.util.stream.StreamValues;

import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class DEBS14GCParser extends Parser {

    @Override
    public List<StreamValues> parse(String input) {
        String[] items = input.split(",");
        return ImmutableList.of(new StreamValues(items[0], Long.parseLong(items[1]), Double.parseDouble(items[2]), Integer.parseInt(items[3]), items[4], items[5], items[6]));
    }

    @Override
    public List<StreamValues> parse(String[] input) {
        return null;
    }

}
