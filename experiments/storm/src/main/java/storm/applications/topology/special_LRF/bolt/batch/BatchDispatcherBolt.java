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
package storm.applications.topology.special_LRF.bolt.batch;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.LinearRoadFConstants;
import storm.applications.hooks.BoltMeterHook;
import storm.applications.topology.special_LRF.TopologyControl;
import storm.applications.topology.special_LRF.types.*;
import storm.applications.util.Multi_Key_value_Map;
import storm.applications.util.config.Configuration;

import java.util.LinkedList;
import java.util.Map;

import static storm.applications.topology.special_LRF.TopologyControl.Field.*;
import static storm.applications.util.config.Configuration.METRICS_ENABLED;


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
    private Multi_Key_value_Map cachePOS;
    //    private Multi_Key_value_Map batchedacc;
//    private Multi_Key_value_Map batcheddaily;
//    private Multi_Key_value_Map batchedtravel;
    private int POSworker;
//    private int accworker;
//    private int dailyworker;
//    private int travelworker;
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
            LinkedList<String[]> batchedPOS = new LinkedList<>();
            LinkedList<String[]> batchedacc = new LinkedList<>();
            LinkedList<String[]> batcheddaily = new LinkedList<>();
            LinkedList<String[]> batchedtravel = new LinkedList<>();

//            batchedPOS = new Multi_Key_value_Map();
//            batchedacc = new Multi_Key_value_Map();
//            batcheddaily = new Multi_Key_value_Map();
//            batchedtravel = new Multi_Key_value_Map();
            cachePOS = new Multi_Key_value_Map();
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
            LinkedList<AccountBalanceRequest> acc = new LinkedList<AccountBalanceRequest>();
            //process
            for (int i = 0; i < acc_size; i++) {
                String[] process = batchedacc.get(i);
                Short time = Short.parseShort(process[1]);
                Integer vid = Integer.parseInt(process[2]);
                Integer qid = new Integer(Integer.parseInt(process[9]));
                acc.add(new AccountBalanceRequest(time, vid, qid));
            }
            if (acc_size > 0)
                this.collector.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID, new Values(acc));

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

                dayl.add(new DailyExpenditureRequest(time, vid, xway, qid, day));
            }
            if (daily_size > 0)
                this.collector.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID, new Values(dayl));

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
        }

        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TopologyControl.POSITION_REPORTS_STREAM_ID, new Fields(PositionReport, PositionReport_Key));
        outputFieldsDeclarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
                new Fields(AccountBalanceRequest));
        outputFieldsDeclarer.declareStream(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
                new Fields(DailyExpenditureRequest));
        outputFieldsDeclarer
                .declareStream(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID,
                        new Fields(TravelTimeRequest));
    }

}
