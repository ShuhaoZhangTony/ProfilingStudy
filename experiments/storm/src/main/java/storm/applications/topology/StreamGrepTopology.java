package storm.applications.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.StreamGrepBolt;
import storm.applications.bolt.batch.BatchStreamGrepBolt;
import storm.applications.grouping.HSP_shuffleGrouping;
import storm.applications.topology.base.BasicTopology;

import java.util.LinkedList;

import static storm.applications.constants.StreamGrepConstants.*;

public class StreamGrepTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(StreamGrepTopology.class);
    private static boolean HSP = false;

    private int StreamGrepBoltThreads;
    private int batch = 0;
    private boolean alo;

    public StreamGrepTopology(String topologyName, Config config) {
        super(topologyName, config);
        StreamGrepBoltThreads = super.config.getInt(Conf.StreamGrepBoltThreads, 1);
    }

    @Override
    public LinkedList ConfigAllocation(int allocation_opt) {
        alo = config.getBoolean("alo", false);//此标识代表topology需要被调度
        return null;
    }

    @Override
    public StormTopology buildTopology() {
        batch = config.getInt("batch");
        HSP = config.getBoolean("hsp", false);
        spout.setFields(new Fields(Field.TEXT));//output of a spouts

        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        LinkedList<Double> partition_ratio = new LinkedList<>();
        if (batch > 1) {

            if (HSP) {
                builder.setBolt(Component.GREP, new BatchStreamGrepBolt(), StreamGrepBoltThreads)
                        .customGrouping(Component.SPOUT, new HSP_shuffleGrouping(spoutThreads, StreamGrepBoltThreads, partition_ratio));
            } else {
                builder.setBolt(Component.GREP, new BatchStreamGrepBolt(), StreamGrepBoltThreads).shuffleGrouping(Component.SPOUT);
            }

        } else {
            if (HSP) {
                for (int i = 0; i < spoutThreads*StreamGrepBoltThreads; i++) {
                    partition_ratio.add(0.5);//simulation
                }
                builder.setBolt(Component.GREP, new StreamGrepBolt(), StreamGrepBoltThreads)
                        .customGrouping(Component.SPOUT, new HSP_shuffleGrouping(spoutThreads, StreamGrepBoltThreads, partition_ratio));
            } else {
                builder.setBolt(Component.GREP, new StreamGrepBolt(), StreamGrepBoltThreads).shuffleGrouping(Component.SPOUT);
            }
        }

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.GREP);

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
