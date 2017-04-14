package flink.applications;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import flink.applications.constants.*;
import flink.applications.constants.WordCountConstants.Component;
import flink.applications.topology.StreamGrepTopology;
import flink.applications.topology.special_LRF.toll.MemoryTollDataStore;
import flink.applications.topology.special_LRF.tools.Helper;
import flink.applications.util.OsUtils;
import flink.applications.util.config.load_TunedConfiguration;
import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkSubmitter;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.configuration.ConfigConstants;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Utility class to run a Storm topology
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FlinkRunner {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkRunner.class);

    private static final String RUN_LOCAL = "local";
    private static final String RUN_REMOTE = "remote";
    //private static final String CFG_PATH = "C:/Users//szhang026//Documents//flink-app//src//main//resources//config//%s.properties";
    private static String CFG_PATH = "/home/flink/flink-app/src/main/resources/config/%s.properties";
    private final flink.applications.AppDriver driver;
    @Parameter
    public List<String> parameters = Lists.newArrayList();
    @Parameter(names = {"-m", "--mode"}, description = "Mode for running the topology")
    public String mode = RUN_LOCAL;
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = false)
    public String application = "word-count";
    @Parameter(names = {"-t", "--topology-name"}, required = false, description = "The name of the topology")
    public String topologyName;
    @Parameter(names = {
            "--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;
    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the topology (local mode only)")
    public int runtimeInSeconds = 50000000;
    // added.
    @Parameter(names = {"-p", "--path"}, description = "Spout path", required = false)
    public String spout_path;
    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = "C:\\metric_output\\test";
    @Parameter(names = {"-cn"}, description = "count number", required = false)
    public int count_number = 1;
    @Parameter(names = {"-n", "--num"}, description = "number of workers", required = false)
    public int num_workers = 1;

    @Parameter(names = {"-bt"}, description = "batch", required = false)
    public int batch = 1;
    @Parameter(names = {"-co"}, description = "TOPOLOGY_WORKER_CHILDOPTS", required = false)
    public String CHILDOPTS = "";

    @Parameter(names = {"-tune"}, description = "tune parallelism", required = false)
    public int tune = 1;//enable tune by default

    @Parameter(names = {"-ct1"}, description = "bolt1.threads", required = false)
    public int threads1 = -1;
    @Parameter(names = {"-ct2"}, description = "bolt2.threads", required = false)
    public int threads2 = -1;
    @Parameter(names = {"-ct3"}, description = "bolt3.threads", required = false)
    public int threads3 = -1;


    private Config config;
    private String cfg;
    private String tollDataStoreClass;

    public FlinkRunner() throws Exception {


        if (OsUtils.isWindows()) {
            CFG_PATH = "src\\main\\resources\\config\\%s.properties";
            metric_path = "metric_output\\test";
        } else {
            CFG_PATH = "/home/flink/flink-app/src/main/resources/config/%s.properties";
            metric_path = "/home/flink/metric_output/test";
        }


        // DateTime dt =DateTime.now();
        tollDataStoreClass = MemoryTollDataStore.class.getName();
        driver = new flink.applications.AppDriver();

        driver.addApp("streamgrep", StreamGrepTopology.class);// final.
        driver.addApp("fraud-detection", flink.applications.topology.FraudDetectionTopology.class);// final.
        driver.addApp("word-count", flink.applications.topology.WordCountTopology.class);// final. n=1
        driver.addApp("log-processing", flink.applications.topology.LogProcessingTopology.class);// final
        driver.addApp("voipstream", flink.applications.topology.VoIPSTREAMTopology.class);// too complicated
        driver.addApp("spike-detection", flink.applications.topology.SpikeDetectionTopology.class);// final
        driver.addApp("traffic-monitoring", flink.applications.topology.TrafficMonitoringTopology.class);//
        driver.addApp("word-count-com", flink.applications.topology.WordCountTopology_communicate.class);
        driver.addApp("word-count-c", flink.applications.topology.WordCountTopology_compute.class);
        driver.addApp("word-count-m", flink.applications.topology.WordCountTopology_mem.class);
        driver.addApp("word-count-cache", flink.applications.topology.WordCountTopology_cache.class);
        driver.addApp("trending-topics", flink.applications.topology.TrendingTopicsTopology.class);// no
        driver.addApp("Smart-grid", flink.applications.topology.SmartGridTopology.class);// rely on tick
        driver.addApp("ads-analytics", flink.applications.topology.AdsAnalyticsTopology.class);// rely on
        driver.addApp("bargain-index", flink.applications.topology.BargainIndexTopology.class);// not
        driver.addApp("linear-road", flink.applications.topology.LinearRoadTopology.class);//
        driver.addApp("linear-road-full", flink.applications.topology.special_LRF.LinearRoadFullTopology.class);//
        driver.addApp("reinforcement-learner", flink.applications.topology.ReinforcementLearnerTopology.class);// don't
        driver.addApp("spam-filter", flink.applications.topology.SpamFilterTopology.class);// data not
        driver.addApp("machine-outlier", flink.applications.topology.MachineOutlierTopology.class);// final
        driver.addApp("sentiment-analysis", flink.applications.topology.SentimentAnalysisTopology.class);// always
        driver.addApp("click-analytics", flink.applications.topology.ClickAnalyticsTopology.class);

    }

    public static void main(String[] args) throws Exception {
        boolean test = false;
        FlinkRunner runner = new FlinkRunner();
        JCommander cmd = new JCommander(runner);
//        String log4jConfPath = "/home/flink/flink-app/src/main/resources/log4j.properties";
//        PropertyConfigurator.configure(log4jConfPath);
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
            System.exit(1);
        }
        runner.run();
    }

    /**
     * Run the topology locally
     *
     * @param topology         The topology to be executed
     * @param topologyName     The name of the topology
     * @param conf             The configurations for the execution
     * @param runtimeInSeconds For how much time the topology will run
     * @throws InterruptedException
     */
    public static void runTopologyLocally(FlinkTopology topology, String topologyName, Config conf,
                                          int runtimeInSeconds) throws Exception {

        flink.applications.util.config.Configuration Conf = flink.applications.util.config.Configuration.fromMap(conf);
        LOG.info("Starting Storm on local mode to run for {} seconds", runtimeInSeconds);
        //LocalCluster cluster = new LocalCluster();
        final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
        LOG.info("Topology {} submitted", topologyName);
        //conf.put(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1024);
        //conf.put(FlinkLocalCluster.SUBMIT_BLOCKING, true); // only required to stabilize integration test
        cluster.submitTopology(topologyName, Conf, topology);
        Thread.sleep((long) runtimeInSeconds * 1000);

        cluster.killTopology(topologyName);
        LOG.info("Topology {} finished", topologyName);

        cluster.shutdown();
        LOG.info("Local Storm cluster was shutdown", topologyName);
    }

    /**
     * Run the topology remotely
     *
     * @param topology     The topology to be executed
     * @param topologyName The name of the topology
     * @param conf         The configurations for the execution
     * @throws AlreadyAliveException
     * @throws InvalidTopologyException
     */
    public static void runTopologyRemotely(FlinkTopology topology, String topologyName, final Config conf)
            throws AlreadyAliveException, InvalidTopologyException {
        //System.out.print("runTopologyRemotely:" + conf.values());
        LOG.info("runTopologyRemotely:" + conf.values());
        FlinkSubmitter.submitTopology(topologyName, conf, topology);
    }

    public static Properties loadProperties(String filename, boolean classpath) throws IOException {
        Properties properties = new Properties();
        InputStream is;

        is = new FileInputStream(filename);
        properties.load(is);
        is.close();

        return properties;
    }


    private void load_TunedConfiguration(String _application, int _tune_option) {
        load_TunedConfiguration l = new load_TunedConfiguration(cfg);
        switch (_tune_option) {
            case 1: {//1core
                l.load_TunedConfiguration_1core(_application);
                break;
            }
            case 2: {//2core
                l.load_TunedConfiguration_2core(_application);
                break;
            }
            case 3: {//4core
                l.load_TunedConfiguration_4core(_application);
                break;
            }
            case 4: {//1socket case, most important one.
                l.load_TunedConfiguration_8core(_application);
                break;
            }
            case 5: {//2sockets
                l.load_TunedConfiguration_16core(_application);
                break;
            }
            case 6: {//4sockets
                l.load_TunedConfiguration_32core(_application);
                break;
            }
            case 7: {//1socket Batch
                l.load_TunedConfiguration_8core_Batch(_application);
                break;
            }
            case 8: {//1socket HP
                l.load_TunedConfiguration_8core_HP(_application);
                break;
            }
            case 9: {//4sockets HP Batch
                l.load_TunedConfiguration_32core_HP_Batch(_application);
                break;
            }
        }

    }

    public void run() throws Exception {
        // Loads the configuration file set by the user or the default
        // configuration

        try {
            // load default configuration
            if (configStr == null) {
                cfg = String.format(CFG_PATH, application);
                load_TunedConfiguration l = new load_TunedConfiguration(cfg);

                if (tune == 0) {
                    //configure threads.
                    switch (application) {
                        case "streamgrep": {
                            l.replaceSelected(cfg, StreamGrepConstants.Conf.StreamGrepBoltThreads,threads1);
                        }
                        case "word-count": {
                            l.replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, threads1);
                            l.replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, threads2);
                            break;
                        }
                        case "fraud-detection": {
                            l.replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, threads1);

                            break;
                        }
                        case "log-processing": {
                            l.replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, threads1);

                            break;
                        }
                        case "spike-detection": {
                            l.replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, threads1);
                            l.replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, threads2);

                            break;
                        }
                        case "voipstream": {
                            l.replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, threads1);
                            l.replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, threads2);

                            break;
                        }
                        case "traffic-monitoring": {
                            l.replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, threads1);
                            l.replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, threads2);

                            break;
                        }
                        case "linear-road-full": {
                            l.replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, threads1);
                            l.replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, threads2);
                            l.replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, threads3);

                            break;
                        }
                    }
                } else {
                    load_TunedConfiguration(application, tune);
                }

                LOG.info("1. Loaded default configuration file {}", cfg);
                Properties p = loadProperties(cfg, (configStr == null));

                config = flink.applications.util.config.Configuration.fromProperties(p);
                config.put("metrics.output", metric_path);
                config.put("batch", batch);
                config.put("count_number", count_number);
                config.put("spout.path", spout_path);
                config.put("runtimeInSeconds", runtimeInSeconds);
                config.put(Helper.TOLL_DATA_STORE_CONF_KEY, this.tollDataStoreClass);
            } else {
                config = flink.applications.util.config.Configuration.fromStr(configStr);
                LOG.info("Loaded configuration from command line argument");
            }
            config.put("AppName", application);
        } catch (IOException ex) {
            LOG.error("Unable to load configuration file", ex);
            throw new RuntimeException("Unable to load configuration file", ex);
        }

        // Get the descriptor for the given application
        flink.applications.AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null)

        {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // In case no topology names is given, create one
        if (topologyName == null)

        {
            //topologyName = String.format("%s-%d", application, new Random().nextInt());
            topologyName = String.format("%s", application);
        }

        // Get the topology and execute on Storm
        FlinkTopology stormTopology = app.getTopology(topologyName, config);
        switch (mode)

        {
            case RUN_LOCAL:
                runTopologyLocally(stormTopology, topologyName, config, runtimeInSeconds);
                break;
            case RUN_REMOTE:
                runTopologyRemotely(stormTopology, topologyName, config);
                break;
            default:
                throw new RuntimeException("Valid running modes are 'local' and 'remote'");
        }
    }
}
