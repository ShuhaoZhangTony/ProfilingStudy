package storm.applications.topology.special_LRF;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.WordCountConstants;
import storm.applications.topology.base.BasicTopology;
import storm.applications.topology.special_LRF.bolt.*;
import storm.applications.topology.special_LRF.bolt.batch.*;
import storm.applications.topology.special_LRF.model.Accident;
import storm.applications.topology.special_LRF.model.AccidentImmutable;
import storm.applications.topology.special_LRF.model.VehicleInfo;
import storm.applications.topology.special_LRF.tools.StopWatch;
import storm.applications.topology.special_LRF.types.AccountBalanceRequest;
import storm.applications.topology.special_LRF.types.DailyExpenditureRequest;
import storm.applications.topology.special_LRF.types.PositionReport;
import storm.applications.topology.special_LRF.types.TravelTimeRequest;
import storm.applications.topology.special_LRF.types.util.SegmentIdentifier;
import storm.applications.topology.special_LRF.util.aeolus.utils.TimestampMerger;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static storm.applications.constants.LinearRoadFConstants.Conf;
import static storm.applications.constants.LinearRoadFConstants.PREFIX;

/**
 * @author mayconbordin
 */
public class LinearRoadFullTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LinearRoadFullTopology.class);

    private int accidentBoltThreads;
    private int dailyExpBoltThreads;
    private int tollBoltThreads;
    private int DispatcherBoltThreads;
    private int AverageSpeedThreads;
    private int CountThreads;
    private int LatestAverageVelocityThreads;
    private int AccidentNotificationBoltThreads;
    private int AccountBalanceBoltThreads;
    private int batch = 0;

    public LinearRoadFullTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        DispatcherBoltThreads = config.getInt(Conf.DispatcherBoltThreads, 1);
        AverageSpeedThreads = config.getInt(Conf.AverageSpeedThreads, 1);
        CountThreads = config.getInt(Conf.CountThreads, 1);
        LatestAverageVelocityThreads = config.getInt(Conf.LatestAverageVelocityThreads, 1);
        tollBoltThreads = config.getInt(Conf.tollBoltThreads, 1);
        accidentBoltThreads = config.getInt(Conf.accidentBoltThreads, 1);
        AccidentNotificationBoltThreads = config.getInt(Conf.AccidentNotificationBoltThreads, 1);
        AccountBalanceBoltThreads = config.getInt(Conf.AccountBalanceBoltThreads, 1);
        dailyExpBoltThreads = config.getInt(Conf.dailyExpBoltThreads, 1);

    }

    @Override
    public StormTopology buildTopology() {
        // builder.setSpout("inputEventInjector", new InputEventInjectorSpout(), 1);//For the moment we keep just one input injector spout
        batch = config.getInt("batch");


        List<String> fields = new LinkedList<String>(Arrays.asList(TopologyControl.XWAY_FIELD_NAME,
                TopologyControl.DIRECTION_FIELD_NAME));

        spout.setFields(new Fields(WordCountConstants.Field.TEXT));//output of a spouts
        builder.setSpout(TopologyControl.SPOUT_NAME, spout, spoutThreads);


        if (batch == 1) {
            builder.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new DispatcherBolt(), DispatcherBoltThreads)
                    .shuffleGrouping(TopologyControl.SPOUT_NAME);

            builder.setBolt(TopologyControl.AVERAGE_SPEED_BOLT_NAME, new AverageVehicleSpeedBolt(), AverageSpeedThreads)
                    .fieldsGrouping(
                            TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID,
                            new Fields(TopologyControl.XWAY_FIELD_NAME, TopologyControl.SEGMENT_FIELD_NAME,
                                    TopologyControl.DIRECTION_FIELD_NAME));
            builder.setBolt(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, new LatestAverageVelocityBolt(), LatestAverageVelocityThreads)
                    .fieldsGrouping(
                            TopologyControl.AVERAGE_SPEED_BOLT_NAME,
                            TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID,
                            SegmentIdentifier.getSchema());
            builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, new AccidentDetectionBolt(), accidentBoltThreads)
                    .fieldsGrouping(
                            TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID,
                            new Fields(TopologyControl.XWAY_FIELD_NAME,
                                    TopologyControl.DIRECTION_FIELD_NAME));
            builder.setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, new TollNotificationBolt(), tollBoltThreads)

                    .allGrouping(TopologyControl.COUNT_VEHICLES_BOLT_NAME, TopologyControl.CAR_COUNTS_STREAM_ID)

                    .fieldsGrouping(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME,
                            TopologyControl.LAVS_STREAM_ID,
                            new Fields(fields))

                    .fieldsGrouping(
                            TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
                            TopologyControl.ACCIDENTS_STREAM_ID,
                            new Fields(
                                    TopologyControl.POS_REPORT_FIELD_NAME,
                                    TopologyControl.SEGMENT_FIELD_NAME))
                    // TopologyControl.ACCIDENT_INFO_FIELD_NAME))

                    .fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID,
                            new Fields(fields));
            builder.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, new AccidentNotificationBolt(), AccidentNotificationBoltThreads)
                    .fieldsGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
                            TopologyControl.ACCIDENTS_STREAM_ID, // streamId
                            new Fields(TopologyControl.POS_REPORT_FIELD_NAME))

                    .fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID, // streamId
                            new Fields(TopologyControl.XWAY_FIELD_NAME,
                                    TopologyControl.DIRECTION_FIELD_NAME));
            builder.setBolt(TopologyControl.COUNT_VEHICLES_BOLT_NAME,
                    new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX), CountThreads)
                    .fieldsGrouping(
                            TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
                            SegmentIdentifier.getSchema());
            builder.setBolt(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME, new AccountBalanceBolt(), AccountBalanceBoltThreads)

                    .fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
                            new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))

                    .fieldsGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,//same vid go to same account balance.
                            TopologyControl.TOLL_ASSESSMENTS_STREAM_ID,
                            new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME));
            builder.setBolt(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME, new DailyExpenditureBolt(), dailyExpBoltThreads)
                    .shuffleGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID);
        } else {
            builder.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new BatchDispatcherBolt(), DispatcherBoltThreads)
                    .shuffleGrouping(TopologyControl.SPOUT_NAME);
            builder.setBolt(TopologyControl.AVERAGE_SPEED_BOLT_NAME, new BatchAverageVehicleSpeedBolt(), AverageSpeedThreads)
                    .fieldsGrouping(
                            TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID, new Fields(TopologyControl.Field.PositionReport_Key)
                    );
            builder.setBolt(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, new BatchLatestAverageVelocityBolt(), LatestAverageVelocityThreads)
                    .fieldsGrouping(
                            TopologyControl.AVERAGE_SPEED_BOLT_NAME,
                            TopologyControl.LAST_AVERAGE_SPEED_STREAM_ID, new Fields(TopologyControl.Field.AvgVehicleSpeedTuple_Key));
            builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, new BatchAccidentDetectionBolt(), accidentBoltThreads)
                    .fieldsGrouping(
                            TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID, new Fields(TopologyControl.Field.PositionReport_Key));
            builder.setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, new BatchTollNotificationBolt(), tollBoltThreads)

                    .allGrouping(TopologyControl.COUNT_VEHICLES_BOLT_NAME, TopologyControl.CAR_COUNTS_STREAM_ID)

                    .globalGrouping(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME,
                            TopologyControl.LAVS_STREAM_ID)

                    .globalGrouping(
                            TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
                            TopologyControl.ACCIDENTS_STREAM_ID)
                    // TopologyControl.ACCIDENT_INFO_FIELD_NAME))

                    .globalGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID);
            builder.setBolt(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, new BatchAccidentNotificationBolt(), AccidentNotificationBoltThreads)
                    .globalGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
                            TopologyControl.ACCIDENTS_STREAM_ID)

                    .globalGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.POSITION_REPORTS_STREAM_ID);
            builder.setBolt(TopologyControl.COUNT_VEHICLES_BOLT_NAME,
                    new BatchCountVehiclesBolt(), CountThreads)
                    .fieldsGrouping(
                            TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
                            new Fields(TopologyControl.Field.PositionReport_Key));
            builder.setBolt(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME, new BatchAccountBalanceBolt(), AccountBalanceBoltThreads)

                    .globalGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID)

                    .globalGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,//same vid go to same account balance.
                            TopologyControl.TOLL_ASSESSMENTS_STREAM_ID);
            builder.setBolt(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME, new BatchDailyExpenditureBolt(), dailyExpBoltThreads)
                    .shuffleGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME,
                            TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID);
        }

/* Tony: Replaced by sink
   builder.setBolt(TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME,
                new FileSinkBolt(topologyNamePrefix + "_toll"), 1).allGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
                TopologyControl.TOLL_ASSESSMENTS_STREAM_ID);
        builder.setBolt(TopologyControl.ACCIDENT_FILE_WRITER_BOLT_NAME, new FileSinkBolt(topologyNamePrefix + "_acc"),
                1).allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENT_INFO_STREAM_ID);

        builder.setBolt(TopologyControl.ACCOUNT_BALANCE_FILE_WRITER_BOLT_NAME,
                new FileSinkBolt(topologyNamePrefix + "_bal"), 1).allGrouping(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME);
       builder.setBolt(TopologyControl.DAILY_EXPEDITURE_FILE_WRITER_BOLT_NAME,
               new FileSinkBolt(topologyNamePrefix + "_exp"), 1).allGrouping(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME);
*/
        builder.setBolt(WordCountConstants.Component.SINK, sink, sinkThreads)
                .globalGrouping(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
                        TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)

                .globalGrouping(TopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME,
                        TopologyControl.ACCIDENTS_NOIT_STREAM_ID)

                .globalGrouping(TopologyControl.ACCOUNT_BALANCE_BOLT_NAME,
                        TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID)

                .globalGrouping(TopologyControl.DAILY_EXPEDITURE_BOLT_NAME,
                        TopologyControl.DAILY_EXPEDITURE_OUTPUT_STREAM_ID);

        config.registerSerialization(PositionReport.class);
        config.registerSerialization(AccountBalanceRequest.class);
        config.registerSerialization(DailyExpenditureRequest.class);
        config.registerSerialization(TravelTimeRequest.class);
        config.registerSerialization(Accident.class);
        config.registerSerialization(VehicleInfo.class);
        config.registerSerialization(StopWatch.class);
        config.registerSerialization(AccidentImmutable.class);
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
