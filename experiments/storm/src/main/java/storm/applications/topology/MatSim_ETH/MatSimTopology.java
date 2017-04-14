package storm.applications.topology.MatSim_ETH;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.applications.topology.base.BasicTopology;
import storm.applications.topology.MatSim_ETH.TopologyControl;
import storm.applications.topology.MatSim_ETH.bolt.DispatcherBolt;

import static storm.applications.constants.WordCountConstants.*;

public class MatSimTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(MatSimTopology.class);
    private int DispatcherBoltThreads=1;

    public MatSimTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

    }

    @Override
    public StormTopology buildTopology() {

        spout.setFields(new Fields(Field.TEXT));//output of a spouts

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        builder.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new DispatcherBolt(), DispatcherBoltThreads)
                .shuffleGrouping(TopologyControl.SPOUT_NAME);


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
