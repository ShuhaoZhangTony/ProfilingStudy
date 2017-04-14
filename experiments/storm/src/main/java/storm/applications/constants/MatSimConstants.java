package storm.applications.constants;

/**
 * @author mayconbordin
 */
public interface MatSimConstants extends BaseConstants {
    String PREFIX = "ms";
    public static final String CONFIG_FILENAME = "linearroad.properties";
    public static final String LINEAR_HISTORY = "linear-history-file";
    public static final String LINEAR_CAR_DATA_POINTS = "linear-cardatapoints-file";
    public static final long POS_EVENT_TYPE = 0;
    public static final long ACC_BAL_EVENT_TYPE = 2;
    public static final long DAILY_EXP_EVENT_TYPE = 3;
    public static final long TRAVELTIME_EVENT_TYPE = 4;
    public static final long NOV_EVENT_TYPE = -5;
    public static final long LAV_EVENT_TYPE = -6;
    public static final long TOLL_EVENT_TYPE = 7;
    public static final long ACCIDENT_EVENT_TYPE = -8;
    public static final String LINEAR_DB_HOST = "linear-db-host";
    public static final String LINEAR_DB_PORT = "linear-db-port";
    public static final int HISTORY_LOADING_NOTIFIER_PORT = 2233;
    public static final String CLEAN_START = "clean-start";
    public static final String HISTORY_COMPONENT_HOST = "localhost"; //This is strictly a temporary value. Must find a way to

    interface Field {
        String TIMESTAMP = "timestamp";
        String VEHICLE_ID = "vehicleId";
        String SPEED = "speed";
        String EXPRESSWAY = "expressway";
        String LANE = "lane";
        String DIRECTION = "direction";
        String SEGMENT = "segment";
        String POSITION = "position";
    }
    //get the correct location of the history loading component.

    interface Conf extends BaseConf {

        String segstatBoltThreads = "lrf.segstat.threads";
        String accidentBoltThreads = "lrf.accident.threads";
        String tollBoltThreads = "lrf.toll.threads";
        String dailyExpBoltThreads = "lrf.dailyExp.threads";
        String xways = "lr.xways";
        String DispatcherBoltThreads = "lrf.dispatch.threads";
        String LatestAverageVelocityThreads = "lrf.latest.threads";
        String AccidentNotificationBoltThreads = "lrf.accidentnoti.threads";
        String AccountBalanceBoltThreads = "lrf.accno.threads";
        String AverageSpeedThreads = "lrf.average.threads";
        String CountThreads = "lrf.count.threads";
    }

    interface Component extends BaseComponent {

    }
}
