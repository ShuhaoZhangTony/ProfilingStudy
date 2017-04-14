package spark.applications.util.geoip;

import spark.applications.constants.BaseConstants.BaseConf;
import spark.applications.util.config.Configuration;


/**
 * @author mayconbordin
 */
public class IPLocationFactory {
    public static final String GEOIP2 = "geoip2";

    public static IPLocation create(String name, Configuration config) {
        if (name.equals(GEOIP2)) {
            return new GeoIP2Location(config.getString(BaseConf.GEOIP2_DB));
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
