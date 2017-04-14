package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.BargainIndexBolt;
import flink.applications.bolt.VwapBolt;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.BargainIndexConstants.*;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BargainIndexTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BargainIndexBolt.class);

    private int vwapThreads;
    private int bargainIndexThreads;

    public BargainIndexTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        vwapThreads = config.getInt(Conf.VWAP_THREADS, 1);
        bargainIndexThreads = config.getInt(Conf.BARGAIN_INDEX_THREADS, 1);
    }

    @Override
    public FlinkTopology buildTopology() {
        spout.setFields(Stream.TRADES, new Fields(Field.STOCK, Field.PRICE, Field.VOLUME, Field.DATE, Field.INTERVAL));
        spout.setFields(Stream.QUOTES, new Fields(Field.STOCK, Field.PRICE, Field.VOLUME, Field.DATE, Field.INTERVAL));

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.VWAP, new VwapBolt(), vwapThreads)
                .fieldsGrouping(Component.SPOUT, Stream.TRADES, new Fields(Field.STOCK));

        builder.setBolt(Component.BARGAIN_INDEX, new BargainIndexBolt(), bargainIndexThreads)
                .fieldsGrouping(Component.VWAP, new Fields(Field.STOCK))
                .fieldsGrouping(Component.SPOUT, Stream.QUOTES, new Fields(Field.STOCK));

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .fieldsGrouping(Component.BARGAIN_INDEX, new Fields(Field.STOCK));

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
