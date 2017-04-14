package storm.applications.spout.generator;

import storm.applications.constants.BaseConstants;
import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;

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
