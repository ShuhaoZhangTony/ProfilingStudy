package flink.applications.topology.PrimitiveOperators;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.primitiveOperator.AggregateBolt;
import flink.applications.bolt.primitiveOperator.ReductionBolt;
import flink.applications.constants.primitiveOperator.AggregateConstants;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.primitiveOperator.AggregateConstants.*;

/**
 * Created by szhang026 on 8/10/2015.
 */
public class WindowAggregateTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FilterTopology.class);
    private int aggThreads;
    private int recThreads;
    private int counterFrequency;

    public WindowAggregateTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        aggThreads = config.getInt(AggregateConstants.Conf.AGGREGATE_THREADS, 1);
        recThreads = config.getInt(AggregateConstants.Conf.REDUCTION_THREADS, 1);
        counterFrequency = config.getInt(AggregateConstants.Conf.COUNTER_FREQ, 60);
    }


    @Override
    public FlinkTopology buildTopology() {
        spout.setFields(new Fields(Field.LONG));
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        //builder.setBolt(Component.AGGREGATE, new AggregateBolt(counterFrequency), aggThreads).fieldsGrouping(Component.SPOUT, new Fields(Field.LONG));
        builder.setBolt(Component.AGGREGATE, new AggregateBolt(counterFrequency), aggThreads).shuffleGrouping(Component.SPOUT);
        builder.setBolt(Component.REDUCTION, new ReductionBolt(counterFrequency), recThreads).globalGrouping(Component.AGGREGATE);
        builder.setBolt(Component.SINK, sink, sinkThreads).globalGrouping(Component.REDUCTION);
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
