package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.StreamGrepBolt;
import flink.applications.bolt.batch.BatchStreamGrepBolt;
import flink.applications.constants.StreamGrepConstants;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static flink.applications.constants.StreamGrepConstants.PREFIX;

public class StreamGrepTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StreamGrepTopology.class);
    private static boolean HSP = false;

    private int StreamGrepBoltThreads;
    private int batch = 0;
    private boolean alo;

    public StreamGrepTopology(String topologyName, Config config) {
        super(topologyName, config);
        StreamGrepBoltThreads = super.config.getInt(StreamGrepConstants.Conf.StreamGrepBoltThreads, 1);
    }

    @Override
    public FlinkTopology buildTopology() {
        batch = config.getInt("batch");
        HSP = config.getBoolean("hsp", false);
        spout.setFields(new Fields(StreamGrepConstants.Field.TEXT));//output of a spouts

        builder.setSpout(StreamGrepConstants.Component.SPOUT, spout, spoutThreads);
        LinkedList<Double> partition_ratio = new LinkedList<>();
        if (batch > 1) {

                builder.setBolt(StreamGrepConstants.Component.GREP, new BatchStreamGrepBolt(), StreamGrepBoltThreads).shuffleGrouping(StreamGrepConstants.Component.SPOUT);


        } else {

                builder.setBolt(StreamGrepConstants.Component.GREP, new StreamGrepBolt(), StreamGrepBoltThreads).shuffleGrouping(StreamGrepConstants.Component.SPOUT);

        }

        builder.setBolt(StreamGrepConstants.Component.SINK, sink, sinkThreads)
                .shuffleGrouping(StreamGrepConstants.Component.GREP);

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
