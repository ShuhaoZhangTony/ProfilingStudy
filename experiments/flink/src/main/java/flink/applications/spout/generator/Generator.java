package flink.applications.spout.generator;

import flink.applications.constants.BaseConstants;
import flink.applications.util.config.Configuration;
import flink.applications.util.stream.StreamValues;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Generator {
    protected Configuration config;
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    public void initialize(Configuration config) {
        this.config = config;
    }

    public abstract StreamValues generate();
}
