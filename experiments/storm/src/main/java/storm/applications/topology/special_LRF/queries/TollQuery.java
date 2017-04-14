/*
 * #!
 * %
 * Copyright (C) 2014 - 2015 Humboldt-Universität zu Berlin
 * %
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #_
 */
package storm.applications.topology.special_LRF.queries;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import storm.applications.topology.special_LRF.TopologyControl;
import storm.applications.topology.special_LRF.bolt.*;
import storm.applications.topology.special_LRF.types.PositionReport;
import storm.applications.topology.special_LRF.types.internal.AvgSpeedTuple;
import storm.applications.topology.special_LRF.types.internal.AvgVehicleSpeedTuple;
import storm.applications.topology.special_LRF.types.util.PositionIdentifier;
import storm.applications.topology.special_LRF.types.util.SegmentIdentifier;
import storm.applications.topology.special_LRF.util.aeolus.sinks.FileFlushSinkBolt;
import storm.applications.topology.special_LRF.util.aeolus.spouts.DataDrivenStreamRateDriverSpout;
import storm.applications.topology.special_LRF.util.aeolus.spouts.DataDrivenStreamRateDriverSpout.TimeUnit;
import storm.applications.topology.special_LRF.util.aeolus.utils.TimestampMerger;


/**
 * {@link TollQuery} assembles the "Toll Processing" query that must notify vehicles about toll to be paid within 5
 * seconds. Additionally, it assess the toll to be paid later on.
 *
 * @author mjsax
 */
public class TollQuery {

    public static StormTopology createTopology(String notificationsOutput, String assessmentsOutput) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TopologyControl.SPOUT_NAME, new DataDrivenStreamRateDriverSpout<Long>(new FileReaderSpout(),
                0, TimeUnit.SECONDS));

        builder.setBolt(TopologyControl.SPLIT_STREAM_BOLT_NAME, new TimestampMerger(new DispatcherBolt(), 0))
                .localOrShuffleGrouping(TopologyControl.SPOUT_NAME);

        builder.setBolt(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME,
                new TimestampMerger(new AccidentDetectionBolt(), PositionReport.TIME_IDX)).fieldsGrouping(
                TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
                PositionIdentifier.getSchema());

        builder.setBolt(TopologyControl.COUNT_VEHICLES_BOLT_NAME,
                new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX)).fieldsGrouping(
                TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
                SegmentIdentifier.getSchema());

        builder.setBolt(TopologyControl.AVERAGE_VEHICLE_SPEED_BOLT_NAME,
                new TimestampMerger(new AverageVehicleSpeedBolt(), PositionReport.TIME_IDX)).fieldsGrouping(
                TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
                new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME));

        builder.setBolt(TopologyControl.AVERAGE_SPEED_BOLT_NAME,
                new TimestampMerger(new AverageSpeedBolt(), AvgVehicleSpeedTuple.MINUTE_IDX)).fieldsGrouping(
                TopologyControl.AVERAGE_VEHICLE_SPEED_BOLT_NAME, SegmentIdentifier.getSchema());

        builder.setBolt(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME,
                new TimestampMerger(new LatestAverageVelocityBolt(), AvgSpeedTuple.MINUTE_IDX)).fieldsGrouping(
                TopologyControl.AVERAGE_SPEED_BOLT_NAME, SegmentIdentifier.getSchema());

        builder
                .setBolt(TopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
                        new TimestampMerger(new TollNotificationBolt(), new TollInputStreamsMerger()))
                .fieldsGrouping(TopologyControl.SPLIT_STREAM_BOLT_NAME, TopologyControl.POSITION_REPORTS_STREAM_ID,
                        new Fields(TopologyControl.VEHICLE_ID_FIELD_NAME))
                .allGrouping(TopologyControl.ACCIDENT_DETECTION_BOLT_NAME, TopologyControl.ACCIDENTS_STREAM_ID)
                .allGrouping(TopologyControl.COUNT_VEHICLES_BOLT_NAME, TopologyControl.CAR_COUNTS_STREAM_ID)
                .allGrouping(TopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, TopologyControl.LAVS_STREAM_ID);

        builder.setBolt(TopologyControl.TOLL_NOTIFICATIONS_FILE_WRITER_BOLT_NAME,
                new FileFlushSinkBolt(notificationsOutput)).localOrShuffleGrouping(
                TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TopologyControl.TOLL_NOTIFICATIONS_STREAM_ID);

        builder.setBolt(TopologyControl.TOLL_ASSESSMENTS_FILE_WRITER_BOLT_NAME,
                new FileFlushSinkBolt(assessmentsOutput)).localOrShuffleGrouping(
                TopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TopologyControl.TOLL_ASSESSMENTS_STREAM_ID);

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            showUsage();
        }

        if (args[0].equals("--local")) {
            if (args.length < 5) {
                showUsage();
            }

            Config c = new Config();
            c.put(FileReaderSpout.INPUT_FILE_NAME, args[1]);

            long runtime = 1000 * Long.parseLong(args[4]);

            LocalCluster lc = new LocalCluster();
            lc.submitTopology(TopologyControl.TOPOLOGY_NAME, c, TollQuery.createTopology(args[2], args[3]));

            Utils.sleep(runtime);

            lc.shutdown();
        } else {
            Config c = new Config();
            c.put(FileReaderSpout.INPUT_FILE_NAME, args[0]);

            StormSubmitter.submitTopology(TopologyControl.TOPOLOGY_NAME, c, TollQuery.createTopology(args[1], args[2]));
        }
    }

    private static void showUsage() {
        System.err.println("Missing arguments. Usage:");
        System.err
                .println("bin/storm jar jarfile.jar [--local] <input> <notificationsOutput> <assessmentsOutput> [<runtime>]");
        System.err.println("  <runtime> is only valid AND required if'--local' is specified");
        System.exit(-1);
    }

}
