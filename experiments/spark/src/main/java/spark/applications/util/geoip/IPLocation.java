package spark.applications.util.geoip;

/**
 * @author mayconbordin
 */
public interface IPLocation {
    public Location resolve(String ip);
}
