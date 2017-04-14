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
package flink.applications.topology.special_LRF.bolt.batch;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.constants.LinearRoadFConstants;
import flink.applications.hooks.BoltMeterHook;
import flink.applications.topology.special_LRF.TopologyControl;
import flink.applications.topology.special_LRF.types.*;
import flink.applications.util.Multi_Key_value_Map;
import flink.applications.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;

import static flink.applications.topology.special_LRF.TopologyControl.Field.*;
import static flink.applications.util.config.Configuration.METRICS_ENABLED;


/**
 * {@link BatchDispatcherBolt} retrieves a stream of {@code <ts,string>} tuples, parses the second CSV attribute and emits an
 * appropriate LRB tuple. The LRB input CSV schema is:
 * {@code Type, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos, QID, S_init, S_end, DOW, TOD, Day}<br />
 * <br />
 * <strong>Output schema:</strong>
 * <ul>
 * <li>{@link PositionReport} (stream: {@link TopologyControl#POSITION_REPORTS_STREAM_ID})</li>
 * <li>{@link AccountBalanceRequest} (stream: {@link TopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID})</li>
 * <li>{@link DailyExpenditureRequest} (stream: {@link TopologyControl#DAILY_EXPEDITURE_REQUESTS_STREAM_ID})</li>
 * <li>{@link TravelTimeRequest} (stream: {@link TopologyControl#TRAVEL_TIME_REQUEST_STREAM_ID})</li>
 * </ul>
 *
 * @author mjsax
 **/
public class BatchDispatcherBolt extends BaseRichBolt {
    private static final long serialVersionUID = 6908631355830501961L;
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchDispatcherBolt.class);
    LinkedList<String[]> batchedPOS = new LinkedList<>();
    LinkedList<String[]> batchedacc = new LinkedList<>();
    LinkedList<String[]> batcheddaily = new LinkedList<>();
    LinkedList<String[]> batchedtravel = new LinkedList<>();
//    private int accworker;
//    private int dailyworker;
//    private int travelworker;
    private Multi_Key_value_Map cachePOS;
    private Multi_Key_value_Map cacheacc;
    private Multi_Key_value_Map cachebat;
    //    private Multi_Key_value_Map batchedacc;
//    private Multi_Key_value_Map batcheddaily;
//    private Multi_Key_value_Map batchedtravel;
    private int POSworker;
    /**
     * The storm provided output collector.
     */
    private OutputCollector collector;
    private int batch;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        Configuration config = Configuration.fromMap(conf);
        if (config.getBoolean(METRICS_ENABLED, false)) {
            topologyContext.addTaskHook(new BoltMeterHook());
        }

        POSworker = config.getInt(LinearRoadFConstants.Conf.AverageSpeedThreads, 1);

        config.registerSerialization(LinkedList.class);
    }

    @Override
    public void execute(Tuple input) {
        String raw = null;
        LinkedList<String> batchedInput = new LinkedList<>();
        try {
            if (input.getValue(0) instanceof LinkedList) {
                batchedInput = (LinkedList<String>) input.getValue(0);
                batch = batchedInput.size();

            } else {
                batchedInput.add((String) input.getValue(0));
                batch = 1;
            }
            String[] token;
            short type;
            //re-create four types of batch input.
            batchedPOS = new LinkedList<>();
            batchedacc = new LinkedList<>();
            batcheddaily = new LinkedList<>();
            batchedtravel = new LinkedList<>();

//            batchedPOS = new Multi_Key_value_Map();
//            batchedacc = new Multi_Key_value_Map();
//            batcheddaily = new Multi_Key_value_Map();
//            batchedtravel = new Multi_Key_value_Map();
            cachePOS = new Multi_Key_value_Map();
            cacheacc = new Multi_Key_value_Map();
            cachebat = new Multi_Key_value_Map();
            for (int i = 0; i < batch; i++) {
                raw = batchedInput.get(i);
                token = raw.split(" ");
                type = Short.parseShort(token[0]);

                switch (type) {
                    case AbstractLRBTuple.position_report: {
                        batchedPOS.add(token);
                        break;
                    }
                    case AbstractLRBTuple.account_balance_request: {
                        batchedacc.add(token);
                        break;
                    }
                    case AbstractLRBTuple.daily_expenditure_request: {
                        batcheddaily.add(token);
                        break;
                    }
                    case AbstractLRBTuple.travel_time_request: {
                        batchedtravel.add(token);
                        break;
                    }
                }
            }

            //process for position_report
            //prepare output.
            int pos_size = batchedPOS.size();
            //LinkedList<PositionReport> ps = new LinkedList<PositionReport>();

            //process
            for (int i = 0; i < pos_size; i++) {
                String[] process = batchedPOS.get(i);
                //common
                //type = Short.parseShort(process[0]);
                Short time = Short.parseShort(process[1]);
                Integer vid = Integer.parseInt(process[2]);
                //
                Integer speed = Integer.parseInt(process[3]);
                Integer xway = Integer.parseInt(process[4]);
                Short lane = Short.parseShort(process[5]);
                Short direction = Short.parseShort(process[6]);
                Short segement = Short.parseShort(process[7]);
                Integer position = Integer.parseInt(process[8]);

                // ps.add(new PositionReport(time, vid, speed, xway, lane, direction, segement, position));
                cachePOS.put(POSworker, new PositionReport(time, vid, speed, xway, lane, direction, segement, position),
                        String.valueOf(xway), String.valueOf(segement), String.valueOf(direction));
            }
            if (pos_size > 0)
                //this.collector.emit(TopologyControl.POSITION_REPORTS_STREAM_ID, new Values(ps));
                cachePOS.emit(TopologyControl.POSITION_REPORTS_STREAM_ID, this.collector);

            //process account_balance_request
            //prepare output.
            int acc_size = batchedacc.size();
            LinkedList<AccountBalanceRequest> acc = new LinkedList<>();
            //process
            for (int i = 0; i < acc_size; i++) {
                String[] process = batchedacc.get(i);
                Short time = Short.parseShort(process[1]);
                Integer vid = Integer.parseInt(process[2]);
                Integer qid = Integer.parseInt(process[9]);
                //cacheacc.put(1, new Field_AccountBalanceRequest(time, vid, qid), String.valueOf(vid));
                //final Field_AccountBalanceRequest ar = new Field_AccountBalanceRequest(time, vid, qid);
                // LOGGER.info("ar:" + ar.toString());
                //acc.add((flink.applications.topology.special_LRF.types.Field_AccountBalanceRequest) ar.clone());
                this.collector.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, new AccountBalanceRequest(time, vid, qid));
                // LOGGER.info(acc.toString());
            }
            //if (acc_size > 0) {
            //  LOGGER.info("acc_size:" + acc_size + ",acc:" + acc.toString());
            //this.collector.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, new Values(acc));
            //}
            //cacheacc.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, this.collector);

            //daily_expenditure_request
            //prepare output.
            int daily_size = batcheddaily.size();
            LinkedList<DailyExpenditureRequest> dayl = new LinkedList<DailyExpenditureRequest>();
            //process
            for (int i = 0; i < daily_size; i++) {
                String[] process = batcheddaily.get(i);
                Short time = Short.parseShort(process[1]);
                Integer vid = Integer.parseInt(process[2]);
                Integer xway = Integer.parseInt(process[4]);
                Integer qid = Integer.parseInt(process[9]);
                Short day = Short.parseShort(process[14]);
                //cachebat.put(1, new DailyExpenditureRequest(time, vid, xway, qid, day));
                final DailyExpenditureRequest dr = new DailyExpenditureRequest(time, vid, xway, qid, day);
                dayl.add(dr);
                //this.collector.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,(new DailyExpenditureRequest(time, vid, xway, qid, day)));
            }
            if (daily_size > 0)
                this.collector.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, new Values(dayl));
            //cachebat.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, this.collector);

            //TRAVEL_TIME_REQUEST_STREAM_ID
            //prepare output.
            int travel_size = batchedtravel.size();
            LinkedList<TravelTimeRequest> travell = new LinkedList<TravelTimeRequest>();
            for (int i = 0; i < travel_size; i++) {
                String[] process = batchedtravel.get(i);
                Short time = Short.parseShort(process[1]);
                Integer vid = Integer.parseInt(process[2]);
                Integer xway = Integer.parseInt(process[4]);
                Integer qid = Integer.parseInt(process[9]);
                Short S_init = Short.parseShort(process[10]);
                Short S_end = Short.parseShort(process[11]);
                Short DOW = Short.parseShort(process[12]);
                Short TOD = Short.parseShort(process[13]);

                travell.add(new TravelTimeRequest(time, vid, xway, qid, S_init, S_end, DOW, TOD));
            }
            this.collector.emit(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, new Values(travell));

        } catch (Exception e) {
            LOGGER.error("Error in line: {}", raw);
            LOGGER.error("StackTrace:", e);
            LOGGER.error(e.getMessage());
            System.exit(-1);
        }

        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TopologyControl.POSITION_REPORTS_STREAM_ID,
                new Fields(PositionReport, PositionReport_Key));
//        outputFieldsDeclarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
//                new Fields(Field_AccountBalanceRequest));
        outputFieldsDeclarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
                AccountBalanceRequest.getSchema());

        outputFieldsDeclarer.declareStream(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
                new Fields(DailyExpenditureRequest));
        outputFieldsDeclarer.declareStream(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID,
                new Fields(TravelTimeRequest));
    }

}
