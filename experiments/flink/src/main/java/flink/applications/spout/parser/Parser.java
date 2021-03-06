package flink.applications.spout.parser;

import flink.applications.util.config.Configuration;
import flink.applications.util.stream.StreamValues;

import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Parser {
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }

    public abstract List<StreamValues> parse(String input);

    public abstract List<StreamValues> parse(String[] input);
}