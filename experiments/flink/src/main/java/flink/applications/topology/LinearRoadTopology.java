package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.AccidentDetectionBolt;
import flink.applications.bolt.DailyExpensesBolt;
import flink.applications.bolt.SegStatBolt;
import flink.applications.bolt.TollBolt;
import flink.applications.constants.LinearRoadConstants;
import flink.applications.constants.WordCountConstants;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.LinearRoadConstants.PREFIX;

/**
 * @author mayconbordin
 */
public class LinearRoadTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LinearRoadTopology.class);
    private int segstatBoltThreads;
    private int accidentBoltThreads;
    private int dailyExpBoltThreads;
    private int tollBoltThreads;

    public LinearRoadTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        segstatBoltThreads = config.getInt(LinearRoadConstants.Conf.segstatBoltThreads, 1);
        accidentBoltThreads = config.getInt(LinearRoadConstants.Conf.accidentBoltThreads, 1);
        tollBoltThreads = config.getInt(LinearRoadConstants.Conf.tollBoltThreads, 1);
        dailyExpBoltThreads = config.getInt(LinearRoadConstants.Conf.dailyExpBoltThreads, 1);

    }

    @Override
    public FlinkTopology buildTopology() {
        // builder.setSpout("inputEventInjector", new InputEventInjectorSpout(), 1);//For the moment we keep just one input injector spout

        spout.setFields("position_report", new Fields(
                "secfromstart",
                "vid",
                "speed",
                "xway",
                "lane",
                "dir",
                "mile",
                "ofst"));
//        spout.setFields("accbal_report", new Fields(
//                "secfromstart",
//                "vid",
//                "qid"));
        spout.setFields("daily_exp", new Fields(
                "secfromstart",
                "vid",
                "xway",
                "qid",
                "day"));

        builder.setSpout(LinearRoadConstants.Component.SPOUT, spout, spoutThreads);

        builder.setBolt("segstatBolt", new SegStatBolt(), segstatBoltThreads)
                .shuffleGrouping(LinearRoadConstants.Component.SPOUT, "position_report");

        builder.setBolt("accidentBolt", new AccidentDetectionBolt(), accidentBoltThreads)
                .shuffleGrouping(LinearRoadConstants.Component.SPOUT, "position_report");

        builder.setBolt("tollBolt", new TollBolt(), tollBoltThreads)
                .shuffleGrouping(LinearRoadConstants.Component.SPOUT, "position_report")
                .shuffleGrouping("segstatBolt", "nov_event")
                .shuffleGrouping("segstatBolt", "lav_event")
                .shuffleGrouping("accidentBolt", "accident_event");

//        builder.setBolt("accbalanceBolt", new AccBalanceBolt(0), 1)
//                .shuffleGrouping(LinearRoadConstants.Component.SPOUT, "accbal_report")
//                .shuffleGrouping("tollBolt", "toll_event");

        builder.setBolt("dailyExpBolt", new DailyExpensesBolt(), dailyExpBoltThreads)
                .shuffleGrouping(LinearRoadConstants.Component.SPOUT, "daily_exp");

//        builder.setBolt("outputBolt", new OutputBolt(), 1)
//                .shuffleGrouping("tollBolt", "toll_event")
//                .shuffleGrouping("accbalanceBolt", "accbalance_event")
//                .shuffleGrouping("dailyExpBolt", "dailyexp_events");
        builder.setBolt(WordCountConstants.Component.SINK, sink, sinkThreads)
                .shuffleGrouping("tollBolt", "toll_event")
//                .shuffleGrouping("accbalanceBolt", "accbalance_event")
                .shuffleGrouping("dailyExpBolt", "dailyexp_events");

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
