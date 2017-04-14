package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.MapMatchingBolt;
import flink.applications.bolt.SpeedCalculatorBolt;
import flink.applications.bolt.batch.BatchMapMatchingBolt;
import flink.applications.bolt.batch.BatchSpeedCalculatorBolt;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static flink.applications.constants.TrafficMonitoringConstants.*;

/**
 * https://github.com/whughchen/RealTimeTraffic
 *
 * @author Chen Guanghua <whughchen@gmail.com>
 */
public class TrafficMonitoringTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoringTopology.class);
    //待分配的组件名称与节点名称的映射关系
    HashMap<String, String> component2Node;
    private int mapMatcherThreads;
    private int speedCalcThreads;
    private int batch;

    public TrafficMonitoringTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        mapMatcherThreads = config.getInt(Conf.MAP_MATCHER_THREADS, 1);
        speedCalcThreads = config.getInt(Conf.SPEED_CALCULATOR_THREADS, 1);

    }

    @Override
    public FlinkTopology buildTopology() {
        batch = config.getInt("batch");
        spout.setFields(new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE));

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        if (batch > 1) {
            System.out.println("Batch Traffic monitoring process");
            builder.setBolt(Component.MAP_MATCHER, new BatchMapMatchingBolt(), mapMatcherThreads)
                    .shuffleGrouping(Component.SPOUT);//not affect.

            builder.setBolt(Component.SPEED_CALCULATOR, new BatchSpeedCalculatorBolt(), speedCalcThreads)
                    .fieldsGrouping(Component.MAP_MATCHER, new Fields(Field.ROAD_ID));
        } else {
            builder.setBolt(Component.MAP_MATCHER, new MapMatchingBolt(), mapMatcherThreads)
                    .shuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.SPEED_CALCULATOR, new SpeedCalculatorBolt(), speedCalcThreads)
                    .fieldsGrouping(Component.MAP_MATCHER, new Fields(Field.ROAD_ID));
        }

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.SPEED_CALCULATOR);
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
