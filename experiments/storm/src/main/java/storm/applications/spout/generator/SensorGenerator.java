package storm.applications.spout.generator;

import org.apache.storm.tuple.Values;
import org.apache.commons.lang3.RandomStringUtils;
import storm.applications.constants.SpikeDetectionConstants.Conf;
import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;

import java.util.Date;
import java.util.Random;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SensorGenerator extends Generator {
    private final Random random = new Random();
    private long count;
    private String deviceID;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);

        count = config.getLong(Conf.GENERATOR_COUNT, 1000000);
        deviceID = RandomStringUtils.randomAlphanumeric(20);
    }

    @Override
    public StreamValues generate() {
        if (count-- > 0) {
            return new StreamValues(deviceID, new Date(), (random.nextDouble() * 10) + 50);
        } else if (count-- == -1) {
            return new StreamValues(new Values(deviceID, -1.0));
        }

        return null;
    }

}
