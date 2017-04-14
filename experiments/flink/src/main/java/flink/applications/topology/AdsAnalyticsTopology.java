package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.RollingCtrBolt;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.AdsAnalyticsConstants.*;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdsAnalyticsTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AdsAnalyticsTopology.class);

    private int ctrThreads;
    private int ctrFrequency;

    public AdsAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        ctrThreads = config.getInt(Conf.CTR_THREADS, 1);
        ctrFrequency = config.getInt(Conf.CTR_EMIT_FREQUENCY, 60);
    }

    @Override
    public FlinkTopology buildTopology() {
        Fields spoutFields = new Fields(Field.QUERY_ID, Field.AD_ID, Field.EVENT);
        spout.setFields(Stream.CLICKS, spoutFields);
        spout.setFields(Stream.IMPRESSIONS, spoutFields);

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.CTR, new RollingCtrBolt(ctrFrequency), ctrThreads)
                .fieldsGrouping(Component.SPOUT, Stream.CLICKS, new Fields(Field.QUERY_ID, Field.AD_ID))
                .fieldsGrouping(Component.SPOUT, Stream.IMPRESSIONS, new Fields(Field.QUERY_ID, Field.AD_ID));

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.CTR);

        return FlinkTopology.createTopology(builder);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }

}
