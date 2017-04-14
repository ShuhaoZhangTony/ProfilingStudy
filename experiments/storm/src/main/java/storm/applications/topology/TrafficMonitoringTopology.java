package storm.applications.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.batch.BatchMapMatchingBolt;
import storm.applications.bolt.batch.BatchSpeedCalculatorBolt;
import storm.applications.bolt.MapMatchingBolt;
import storm.applications.bolt.SpeedCalculatorBolt;
import storm.applications.topology.base.BasicTopology;

import java.util.HashMap;

import static storm.applications.constants.TrafficMonitoringConstants.*;

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
    public StormTopology buildTopology() {
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


        //此标识代表topology需要被调度
        config.put("assigned_flag", "1");
        HashMap<Object, Object> component2Node;
        component2Node = new HashMap<>();

        component2Node.put(Component.MAP_MATCHER, "socket3");
        component2Node.put(Component.SPEED_CALCULATOR, "socket0");

        //具体的组件节点对信息
        config.put("design_map", component2Node);


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
