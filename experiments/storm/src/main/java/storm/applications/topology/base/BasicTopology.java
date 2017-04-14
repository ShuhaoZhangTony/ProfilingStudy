package storm.applications.topology.base;

import org.apache.storm.Config;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;

import java.util.LinkedList;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class BasicTopology extends AbstractTopology {
    protected AbstractSpout spout;
    protected BaseSink sink;
    protected int spoutThreads;
    protected int sinkThreads;

    public BasicTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        spout = loadSpout();
        sink = loadSink();
        // System.out.println(config.values());
        spoutThreads = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS), 1);
        sinkThreads = config.getInt(getConfigKey(BaseConf.SINK_THREADS), 1);
    }
}
