package spark.applications;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.applications.functions.statefulSink;
import spark.applications.jobs.FraudDetectionJob;
import spark.applications.jobs.StreamGrepJob;
import spark.applications.util.OsUtils;
import spark.applications.util.propertiesUtil;

import java.util.List;
import java.util.Random;

/**
 * Utility class to run a Storm topology
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SparkRunner {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

    private static final String RUN_LOCAL = "local";
    private static final String RUN_REMOTE = "remote";

    private final AppDriver driver;
    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = {"-m", "--mode"}, description = "Mode for running the topology")
    public String mode = RUN_LOCAL;

    @Parameter(names = {"-ma"}, description = "URL of master", required = false)
    public String URL_master = "spark://155.69.149.213:7070";

    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = false)
    public String application = "fraud-detection";

    @Parameter(names = {"-t", "--topology-name"}, required = false, description = "The name of the topology")
    public String jobName;

    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the topology (local mode only)")
    public int runtimeInSeconds = 50000000;

    // added.
    @Parameter(names = {"-p", "--path"}, description = "Spout path", required = false)
    public String spout_path;

    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path;

    @Parameter(names = {"-cn"}, description = "count number", required = false)
    public int count_number = 1;
    @Parameter(names = {"-n", "--num"}, description = "number of workers", required = false)
    public int num_workers = 1;


    @Parameter(names = {"-hsp"}, description = "HSP", required = false)
    public boolean hsp = false;
    @Parameter(names = {"-co"}, description = "spark.executor.extraJavaOptions", required = false)
    public String CHILDOPTS = "";

    @Parameter(names = {"-tune"}, description = "tune parallelism", required = false)
    public int tune = 1;//enable tune by default

    @Parameter(names = {"-mf"}, description = "memoryFraction", required = false)
    public double memoryFraction = 0.6;

    @Parameter(names = {"-bt"}, description = "batch", required = false)
    public int batch = 10000;//In Storm or Flink, how many tuples are sent from spout in 1 msecond?

    @Parameter(names = {"-duration"}, description = "Spark-streaming batch duration", required = false)
    private long batch_duration = 1;//default 1 msecond


    public SparkRunner() {
        driver = new AppDriver();
        driver.addApp("streamgrep", StreamGrepJob.class);
        driver.addApp("fraud-detection", FraudDetectionJob.class);
        if (OsUtils.isWindows()) {
            metric_path = "metric_output\\test";
        } else {
            metric_path = "/home/spark/metric_output/test";
        }
    }

    public static void main(String[] args) throws Exception {
//        URL location = SparkRunner.class.getProtectionDomain().getCodeSource().getLocation();
//        System.out.println(location.getFile());
        String log4jConfPath;

        if (OsUtils.isWindows()) {
            log4jConfPath = "src\\main\\resources" + OsUtils.OS_wrapper("log4j.properties");
        } else {
            log4jConfPath = "/home/spark/spark-app/src/main/resources" + OsUtils.OS_wrapper("log4j.properties");
        }

        PropertyConfigurator.configure(log4jConfPath);
        SparkRunner runner = new SparkRunner();
        JCommander cmd = new JCommander(runner);
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
            System.exit(1);
        }
        runner.run();
    }


    public void run() throws InterruptedException {

        // load configuration
        SparkConf sparkConf = propertiesUtil.load(application);
        sparkConf.setAppName("Spark-App");
        sparkConf.setJars(JavaStreamingContext.jarOfClass(SparkRunner.class));

        sparkConf.set("batch", String.valueOf(batch));
        sparkConf.set("spark.executor.extraJavaOptions", CHILDOPTS);
        sparkConf.set("spark.storage.memoryFraction", String.valueOf(memoryFraction));
        sparkConf.set("count_number", String.valueOf(count_number));
        sparkConf.set("metric_path", metric_path);

        switch (mode) {
            case RUN_LOCAL: {
                sparkConf.setMaster("local[*]");
                break;
            }
            case RUN_REMOTE: {
                sparkConf.setMaster(URL_master);
                break;
            }
        }

        // Create the context

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(batch_duration));

        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // In case no job names is given, create one
        if (jobName == null) {
            jobName = String.format("%s-%d", application, new Random().nextInt());
        }

        // Get the application and execute on Spark
        JavaDStream result = app.getJob(ssc, jobName, sparkConf);
        //result.print();
        /**
         * In general, we should implement "sink" here, as this is where the final results are obtained.
         */
        //result.foreachRDD(new sink(ssc,batch,sparkConf));//a batch of works are done..

        //make all RDD to share the same key in order to maintain global states for sink purpose..
        final JavaPairDStream javaPairDStream = result.mapToPair((PairFunction) o -> new Tuple2<>("mykey", o));
        StateSpec ss = StateSpec.function(new statefulSink());
        javaPairDStream.mapWithState(ss);

        ssc.start();
        ssc.awaitTermination();
    }
}
