package flink.applications.topology.PrimitiveOperators;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.primitiveOperator.FilterBolt;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.primitiveOperator.FilterConstants.*;

/**
 * Created by szhang026 on 8/8/2015.
 */
public class FilterTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FilterTopology.class);
    private int fcThreads;

    public FilterTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        fcThreads = config.getInt(Conf.Filter_THREADS, 1);
    }

    @Override
    public FlinkTopology buildTopology() {


//        spout.setFields(new Fields(Field.TEXT));//output of a spouts
//
//        builder.setSpout(Component.SPOUT, spout, spoutThreads);
//
//        builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads).shuffleGrouping(Component.SPOUT);
//
//        builder.setBolt(Component.COUNTER, new WordCountBolt(), wordCountThreads)
//                .fieldsGrouping(Component.SPLITTER, new Fields(Field.WORD));
//
//        builder.setBolt(Component.SINK, sink, sinkThreads)
//                .shuffleGrouping(Component.COUNTER);
//
//        return FlinkTopology.createTopology(builder);

        spout.setFields(new Fields(Field.INT));
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.FILTER, new FilterBolt(), fcThreads).shuffleGrouping(Component.SPOUT);
        builder.setBolt(Component.SINK, sink, sinkThreads).shuffleGrouping(Component.FILTER);
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
