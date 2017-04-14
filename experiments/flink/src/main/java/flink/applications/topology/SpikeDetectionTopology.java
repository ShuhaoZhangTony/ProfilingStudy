package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.MovingAverageBolt;
import flink.applications.bolt.SpikeDetectionBolt;
import flink.applications.bolt.batch.BatchMovingAverageBolt;
import flink.applications.bolt.batch.BatchSpikeDetectionBolt;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.SpikeDetectionConstants.*;

/**
 * Detects spikes in values emitted from sensors.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class SpikeDetectionTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionTopology.class);

    private int movingAverageThreads;
    private int spikeDetectorThreads;
    private int batch;

    public SpikeDetectionTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        movingAverageThreads = config.getInt(Conf.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads = config.getInt(Conf.SPIKE_DETECTOR_THREADS, 1);
    }

    @Override
    public FlinkTopology buildTopology() {
        batch = config.getInt("batch");
        spout.setFields(new Fields(Field.DEVICE_ID, Field.TIMESTAMP, Field.VALUE));

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        if (batch > 1) {
            builder.setBolt(Component.MOVING_AVERAGE, new BatchMovingAverageBolt(), movingAverageThreads)
                    .globalGrouping(Component.SPOUT);//fixed to 1

            builder.setBolt(Component.SPIKE_DETECTOR, new BatchSpikeDetectionBolt(), spikeDetectorThreads)
                    .shuffleGrouping(Component.MOVING_AVERAGE);//not affect.
        } else {
            builder.setBolt(Component.MOVING_AVERAGE, new MovingAverageBolt(), movingAverageThreads)
                    .fieldsGrouping(Component.SPOUT, new Fields(Field.DEVICE_ID));

            builder.setBolt(Component.SPIKE_DETECTOR, new SpikeDetectionBolt(), spikeDetectorThreads)
                    .shuffleGrouping(Component.MOVING_AVERAGE);
        }


        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.SPIKE_DETECTOR);

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
