package storm.applications.topology.PrimitiveOperators;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.primitiveOperator.FilterBolt;
import storm.applications.topology.base.BasicTopology;

import static storm.applications.constants.primitiveOperator.FilterConstants.*;

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
    public StormTopology buildTopology() {


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
//        return builder.createTopology();

        spout.setFields(new Fields(Field.INT));
        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(Component.FILTER, new FilterBolt(), fcThreads).shuffleGrouping(Component.SPOUT);
        builder.setBolt(Component.SINK, sink, sinkThreads).shuffleGrouping(Component.FILTER);
        return builder.createTopology();
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
