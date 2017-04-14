package storm.applications;

import clojure.lang.PersistentArrayMap;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Lists;
import org.apache.storm.*;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;

import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import storm.applications.constants.*;
import storm.applications.constants.WordCountConstants.Component;
import storm.applications.grouping.HSP_shuffleGrouping;
import storm.applications.scheduling.myTuple2;
import storm.applications.topology.*;
import storm.applications.topology.MatSim_ETH.MatSimTopology;
import storm.applications.topology.special_LRF.LinearRoadFullTopology;
import storm.applications.topology.special_LRF.toll.MemoryTollDataStore;
import storm.applications.topology.special_LRF.tools.Helper;
import storm.applications.util.OsUtils;
import storm.applications.util.config.Configuration;
import storm.applications.util.config.load_TunedConfiguration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class StormRunner {
    private static final Logger LOG = LoggerFactory.getLogger(StormRunner.class);

    private static final String RUN_LOCAL = "local";
    private static final String RUN_REMOTE = "remote";
    private static String CFG_PATH = null;

    private final AppDriver driver;
    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = {"-m", "--mode"}, description = "Mode for running the topology")
    public String mode = RUN_LOCAL;

    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = false)
    public String application = "streamgrep";

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
    public String metric_path = "";
    //public String metric_path = "/media/storm/ProfilingData/test";
    @Parameter(names = {"-cn"}, description = "count number", required = false)
    public int count_number = 1;
    @Parameter(names = {"-n", "--num"}, description = "number of workers", required = false)
    public int num_workers = 1;
    // @Parameter(names = {"-st"}, description = "wc.splitter.threads", required
    // = false)
    // public int Sthreads=0;
    @Parameter(names = {"-bt"}, description = "batch", required = false)
    public int batch = 1;
    @Parameter(names = {"-hsp"}, description = "HSP", required = false)
    public boolean hsp = false;

    @Parameter(names = {"-alo"}, description = "custom executor allocation", required = false)
    public boolean alo = true;

    @Parameter(names = {"-alop"}, description = "custom executor allocation plan", required = false)
    public int allocation_plan = 1;

    @Parameter(names = {"-ack"}, description = "acknowledgement", required = false)
    public boolean ack = false;//we disable ack as we work on single machine.

    @Parameter(names = {"-co"}, description = "TOPOLOGY_WORKER_CHILDOPTS", required = false)
    public String CHILDOPTS = "";

    @Parameter(names = {"-tune"}, description = "tune parallelism", required = false)
    public int tune = 4;//disable tune and use 1 socket setting by default.

    @Parameter(names = {"-ct1"}, description = "bolt1.threads", required = false)
    public int threads1 = -1;
    @Parameter(names = {"-ct2"}, description = "bolt2.threads", required = false)
    public int threads2 = -1;
    @Parameter(names = {"-ct3"}, description = "bolt3.threads", required = false)
    public int threads3 = -1;
    @Parameter(names = {"-ct4"}, description = "bolt4.threads", required = false)
    public int threads4 = -1;



    private String tollDataStoreClass;
    public Config config;

    public StormRunner() {

        if (OsUtils.isWindows()) {
            CFG_PATH = "src\\main\\resources\\config\\%s.properties";
            metric_path = "metric_output\\test";
        } else {
            CFG_PATH = "/home/storm/storm-app/src/main/resources/config/%s.properties";
            metric_path = "/home/storm/metric_output/test";
        }
        tollDataStoreClass = MemoryTollDataStore.class.getName();

        driver = new AppDriver();

        driver.addApp("streamgrep", StreamGrepTopology.class);// final.

        driver.addApp("fraud-detection", FraudDetectionTopology.class);// final.

        driver.addApp("word-count", WordCountTopology.class);// final. n=1

        driver.addApp("log-processing", LogProcessingTopology.class);// final

        driver.addApp("voipstream", VoIPSTREAMTopology.class);//
        driver.addApp("spike-detection", SpikeDetectionTopology.class);// final

        driver.addApp("traffic-monitoring", TrafficMonitoringTopology.class);//

        driver.addApp("word-count-com", WordCountTopology_communicate.class);
        driver.addApp("word-count-c", WordCountTopology_compute.class);
        driver.addApp("word-count-m", WordCountTopology_mem.class);
        driver.addApp("word-count-cache", WordCountTopology_cache.class);

        driver.addApp("trending-topics", TrendingTopicsTopology.class);// not

        driver.addApp("Smart-grid", SmartGridTopology.class);// rely on tick
        driver.addApp("ads-analytics", AdsAnalyticsTopology.class);// rely on

        driver.addApp("bargain-index", BargainIndexTopology.class);// not

        driver.addApp("linear-road", LinearRoadTopology.class);//
        driver.addApp("linear-road-full", LinearRoadFullTopology.class);//
        driver.addApp("reinforcement-learner", ReinforcementLearnerTopology.class);// don't

        driver.addApp("spam-filter", SpamFilterTopology.class);// data not

        driver.addApp("machine-outlier", MachineOutlierTopology.class);// final

        driver.addApp("sentiment-analysis", SentimentAnalysisTopology.class);// always

        driver.addApp("click-analytics", ClickAnalyticsTopology.class);

        driver.addApp("matsim", MatSimTopology.class);

    }

    public static void main(String[] args) throws Exception {
        boolean test = false;
        if (test)
            testBasicTopology();
        else {
            StormRunner runner = new StormRunner();
            JCommander cmd = new JCommander(runner);

            try {
                cmd.parse(args);
            } catch (ParameterException ex) {
                System.err.println("Argument error: " + ex.getMessage());
                cmd.usage();
                System.exit(1);
            }
            try {
                runner.run();
            } catch (AlreadyAliveException | InvalidTopologyException ex) {
                LOG.error("Error in running topology remotely", ex);
            } catch (InterruptedException ex) {
                LOG.error("Error in running topology locally", ex);
            }
        }
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
    public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf,
                                          int runtimeInSeconds) throws InterruptedException {
        Configuration Conf = Configuration.fromMap(conf);
        //conf.put(Config.STORM_SCHEDULER, "storm.applications.scheduling.DirectScheduler");
        conf.setNumWorkers(Conf.getInt("num_workers", 1));
        conf.setMaxSpoutPending(Conf.getInt("max_pending", 5000));
        conf.put(Config.STORM_SCHEDULER, "storm.applications.scheduling.FinegrainedScheduler");
        LOG.info("Starting Storm on local mode to run for {} seconds", runtimeInSeconds);
        LocalCluster cluster = new LocalCluster();

        LOG.info("Topology {} submitted", topologyName);
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * 1000);

        cluster.killTopology(topologyName);
        LOG.info("Topology {} finished", topologyName);

        cluster.shutdown();
        LOG.info("Local Storm cluster was shutdown", topologyName);

    }

    public static void testBasicTopology() {
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        daemonConf.put(Config.STORM_SCHEDULER, "storm.applications.scheduling.FinegrainedScheduler");

        mkClusterParam.setDaemonConf(daemonConf);

        /**
         * This is a combination of <code>Testing.withLocalCluster</code> and
         * <code>Testing.withSimulatedTime</code>.
         */
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
            @Override
            public void run(ILocalCluster cluster) throws IOException {
                // build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                int no_spout = 2;
                int no_count = 3;


                builder.setSpout("spout", new TestWordSpout(true), no_spout);
                //builder.setBolt("count", new TestWordCounter(), 4).fieldsGrouping("spout", new Fields("word"));

                LinkedList<Double> partition_ratio = new LinkedList();
                for (int i = 0; i < no_spout; i++) {
                    double total = 1.0;
                    for (int j = 0; j < no_count - 1; j++) {
                        double random = ThreadLocalRandom.current().nextDouble(0.0, total);
                        partition_ratio.add(i * no_count + j, random);
                        total -= random;
                    }
                    partition_ratio.add(i * no_count + no_count - 1, total);
                }

                builder.setBolt("count", new TestWordCounter(), no_count).customGrouping("spout", new HSP_shuffleGrouping(no_spout, no_count, partition_ratio));


                builder.setBolt("global_count", new TestGlobalCount()).globalGrouping("spout");
                builder.setBolt("agg_count", new TestAggregatesCounter()).globalGrouping("count");
                StormTopology topology = builder.createTopology();

                // complete the topology

                // prepare the mock data
                MockedSources mockedSources = new MockedSources();

                mockedSources.addMockData("spout", new Values("nathan"), new Values("bob"), new Values("joey"),
                        new Values("nathan"));

                // prepare the config
                Config conf = new Config();
                conf.setNumWorkers(4);
                //此标识代表topology需要被调度
                conf.put("assigned_flag", "1");
                conf.put("alo", true);
                // <<Which socket, which componet, how many executors>>
                LinkedList component2Node = new LinkedList<>();

                component2Node.add("socket0" + "," + "spout" + "," + 1);
                component2Node.add("socket1" + "," + "spout" + "," + (no_spout - 1));

                component2Node.add("socket3" + "," + "count" + "," + no_count);

                //具体的组件节点对信息
                conf.put("design_map", (component2Node));

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);
                /**
                 * TODO
                 */

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

                // check whether the result is right
                assertTrue(Testing.multiseteq(
                        new Values(new Values("nathan"), new Values("bob"), new Values("joey"), new Values("nathan")),
                        Testing.readTuples(result, "spout")));
                assertTrue(Testing.multiseteq(new Values(new Values("nathan", 1), new Values("nathan", 2),
                        new Values("bob", 1), new Values("joey", 1)), Testing.readTuples(result, "count")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2), new Values(3), new Values(4)),
                        Testing.readTuples(result, "global_count")));
                assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2), new Values(3), new Values(4)),
                        Testing.readTuples(result, "agg_count")));
            }

            private void assertTrue(boolean rt) {
                // TODO Auto-generated method stub
                if (rt != true)
                    LOG.error("Something wrong!\n");
            }

        });
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
    public static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf)
            throws AlreadyAliveException, InvalidTopologyException {

        // conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

        Configuration Conf = Configuration.fromMap(conf);
        conf.setMaxSpoutPending(Conf.getInt("max_pending", 5000));
        conf.setNumWorkers(Conf.getInt("num_workers", 1));
        // This will simply log all Metrics received into
        // $STORM_HOME/logs/metrics.log on one or more worker nodes.
        // conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

        System.out.println("max pending;" + conf.get("topology.max.spout.pending"));
        System.out.println("metrics.output:" + Conf.getString("metrics.output"));
        System.out.println("NumWorkers:" + Conf.getInt("num_workers"));

        try {
            StormSubmitter.submitTopology(topologyName, conf, topology);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Properties loadProperties(String filename, boolean classpath) throws IOException {
        Properties properties = new Properties();
        InputStream is;
        is = new FileInputStream(filename);
        properties.load(is);
        is.close();

        return properties;
    }


    private void load_TunedConfiguration(String _application, int _tune_option, int _batch) {
        load_TunedConfiguration l = new load_TunedConfiguration(config);
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
                l.load_TunedConfiguration_8core_Batch(_application, _batch);
                break;
            }
            case 8: {//1socket HP
                l.load_TunedConfiguration_8core_HP(_application);
                break;
            }
            case 10: {//4sockets HP Batch
                l.load_TunedConfiguration_32core_HP_Batch(_application);
                break;
            }
        }

    }

    public void run() throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        // Loads the configuration file set by the user or the default
        // configuration
        try {
            // load default configuration
            if (configStr == null) {

                String cfg = String.format(CFG_PATH, application);
                LOG.info("1. Loaded default configuration file {}", cfg);
                Properties p = loadProperties(cfg, (configStr == null));

                config = Configuration.fromProperties(p);
                config.setDebug(false);
                // LOG.info("2. Loaded default configuration file {}", cfg);
                config.put("batch", batch);
                config.put("hsp", hsp);
                config.put("count_number", count_number);
                config.put("spout.path", spout_path);
                config.put("metrics.output", metric_path);
                config.put("num_workers", num_workers);
                config.put("runtimeInSeconds", runtimeInSeconds);
                config.put(Helper.TOLL_DATA_STORE_CONF_KEY, this.tollDataStoreClass);
                config.put("allocation_plan",allocation_plan);
                config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, CHILDOPTS);

                config.put("alo", alo);
                if (alo)
                    config.put(Config.STORM_SCHEDULER, "storm.applications.scheduling.FinegrainedScheduler");


                if (tune == 0) {
                    //configure threads.
                    switch (application) {
                        case "streamgrep": {
                            config.put(StreamGrepConstants.Conf.StreamGrepBoltThreads,threads1);
                        }

                        case "traffic-monitoring": {
                            config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, threads1);
                            config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, threads2);
                            break;
                        }
                        case "linear-road-full": {
                            config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, threads1);
                            config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, threads2);
                            config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, threads3);
                            break;
                        }
/*
                        case "word-count": {
                            config.put(WordCountConstants.Conf.SPLITTER_THREADS, threads1);
                            config.put(WordCountConstants.Conf.COUNTER_THREADS, threads2);
                            break;
                        }
                        case "fraud-detection": {
                            config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, threads1);
                            break;
                        }
                        case "log-processing": {
                            config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, threads1);
                            break;
                        }
                        case "spike-detection": {
                            config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, threads1);
                            config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, threads2);
                            break;
                        }
                        case "voipstream": {
                            config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, threads1);
                            config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, threads2);
                            break;
                        }
*/
                    }
                } else {
                    load_TunedConfiguration(application, tune, batch);
                }
            } else {
                config = Configuration.fromStr(configStr);
                LOG.info("Loaded configuration from command line argument");
            }
        } catch (IOException ex) {
            LOG.error("Unable to load configuration file", ex);
            throw new RuntimeException("Unable to load configuration file", ex);
        }

        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // In case no topology names is given, create one
        if (topologyName == null) {
            topologyName = application;
        }

        // Get the topology and execute on Storm
        StormTopology stormTopology = app.getTopology(topologyName, config);
        config.put("design_map", app.allocation);

        if(!ack)
            config.put("topology.acker.executors", 0);
        switch (mode) {
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
