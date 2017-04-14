package spark.applications;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spark.applications.jobs.AbstractJob;
import spark.applications.util.config.Configuration;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * @author mayconbordin
 */
public class AppDriver {
    private static final Logger LOG = LoggerFactory.getLogger(AppDriver.class);
    private final Map<String, AppDescriptor> applications;

    public AppDriver() {
        applications = new HashMap<>();
    }

    public void addApp(String name, Class<? extends AbstractJob> cls) {
        applications.put(name, new AppDescriptor(cls));
    }

    public AppDescriptor getApp(String name) {
        return applications.get(name);
    }

    public static class AppDescriptor {
        private final Class<? extends AbstractJob> cls;

        public AppDescriptor(Class<? extends AbstractJob> cls) {
            this.cls = cls;
        }

        public JavaDStream getJob(JavaStreamingContext ssc, String topologyName, SparkConf config) {
            try {
                Constructor c = cls.getConstructor(String.class, SparkConf.class);
                LOG.info("Loaded topology {}", cls.getCanonicalName());

                AbstractJob topology = (AbstractJob) c.newInstance(topologyName, config);
                topology.initialize(ssc);
                return topology.buildJob(ssc);
            } catch (ReflectiveOperationException ex) {
                LOG.error("Unable to load topology class", ex);
            }
            return null;
        }
    }
}
