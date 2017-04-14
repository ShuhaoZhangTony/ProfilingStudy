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
package flink.applications.topology.special_LRF.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import flink.applications.hooks.BoltMeterHook;
import flink.applications.topology.special_LRF.TopologyControl;
import flink.applications.topology.special_LRF.types.*;
import flink.applications.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static flink.applications.util.config.Configuration.METRICS_ENABLED;


/**
 * {@link DispatcherBolt} retrieves a stream of {@code <ts,string>} tuples, parses the second CSV attribute and emits an
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
public class DispatcherBolt extends BaseRichBolt {
    private static final long serialVersionUID = 6908631355830501961L;
    private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherBolt.class);

    /**
     * The storm provided output collector.
     */
    private OutputCollector collector;


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        Configuration config = Configuration.fromMap(conf);
        if (config.getBoolean(METRICS_ENABLED, false)) {
            topologyContext.addTaskHook(new BoltMeterHook());
        }
    }

    @Override
    public void execute(Tuple input) {
        String raw = null;


		/*   byte typeField = Byte.parseByte(fields[0]);
        *  Need to calculate the offset value
                String offset = "" + ((short) (Integer.parseInt(fields[8]) - (Integer.parseInt(fields[7]) * 5280)));
                //This is a position report (Type=0, Time, VID, Spd, Xway, Lane, Dir, Seg, Pos)
                time = Long.parseLong(fields[1]);
                vid = Integer.parseInt(fields[2]);
                spd = Byte.parseByte(fields[3]);
                xway = Byte.parseByte(fields[4]);
                lane = Byte.parseByte(fields[5]);
                dir = Byte.parseByte(fields[6]);
                mile = Byte.parseByte(fields[7]);
		*   qid = Integer.parseInt(fields[9]);
		*   day = Integer.parseInt(fields[14]);
		*
		*
		*
		*   15 attributes: Type, Time, VID, Spd, XWay, Lane, Dir, Seg, Pos, QID, Sinit, Send, DOW, TOD, and Day.
		*                   0  ,  1  ,  2 ,  3 ,  4  ,  5  ,  6 ,  7 ,   8,   9,    10,   11,  12,  13,  and 14.
		* */


        try {
            raw = input.getString(0);
            //raw = raw.substring(3, raw.length() - 2);

            String[] token = raw.split(" ");

            // common attributes of all input tuples
            short type = Short.parseShort(token[0]);
            Short time = Short.parseShort(token[1]);
            Integer vid = Integer.parseInt(token[2]);

            assert (time.shortValue() == Short.parseShort(token[1]));

            if (type == AbstractLRBTuple.position_report) {
                this.collector.emit(TopologyControl.POSITION_REPORTS_STREAM_ID,
                        new PositionReport(//
                                time,//
                                vid,//
                                new Integer(Integer.parseInt(token[3])), // speed
                                new Integer(Integer.parseInt(token[4])), // xway
                                new Short(Short.parseShort(token[5])), // lane
                                new Short(Short.parseShort(token[6])), // direction
                                new Short(Short.parseShort(token[7])), // segment
                                new Integer(Integer.parseInt(token[8])))); // position
            } else {
                // common attribute of all requests
                Integer qid = new Integer(Integer.parseInt(token[9]));

                switch (type) {
                    case AbstractLRBTuple.account_balance_request:
                        this.collector.emit(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
                                new AccountBalanceRequest(time, vid, qid
//                                        ,
//                                        new PositionReport(//
//                                                time,//
//                                                vid,//
//                                                new Integer(Integer.parseInt(token[3])), // speed
//                                                new Integer(Integer.parseInt(token[4])), // xway
//                                                new Short(Short.parseShort(token[5])), // lane
//                                                new Short(Short.parseShort(token[6])), // direction
//                                                new Short(Short.parseShort(token[7])), // segment
//                                                new Integer(Integer.parseInt(token[8])))
                                ));
                        break;
                    case AbstractLRBTuple.daily_expenditure_request:
                        this.collector.emit(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
                                new DailyExpenditureRequest(time, vid,//
                                        new Integer(Integer.parseInt(token[4])), // xway
                                        qid,//
                                        new Short(Short.parseShort(token[14])))); // day
                        break;
                    case AbstractLRBTuple.travel_time_request:
                        this.collector.emit(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID,
                                new TravelTimeRequest(time, vid,//
                                        new Integer(Integer.parseInt(token[4])), // xway
                                        qid,//
                                        new Short(Short.parseShort(token[10])), // S_init
                                        new Short(Short.parseShort(token[11])), // S_end
                                        new Short(Short.parseShort(token[12])), // DOW
                                        new Short(Short.parseShort(token[13])))); // TOD
                        break;
                    default:
                        LOGGER.error("Unkown tuple type: {}", new Short(type));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error in line: {}", raw);
            LOGGER.error("StackTrace:", e);
        }

        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TopologyControl.POSITION_REPORTS_STREAM_ID, PositionReport.getSchema());
        outputFieldsDeclarer.declareStream(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID,
                AccountBalanceRequest.getSchema());
        outputFieldsDeclarer.declareStream(TopologyControl.DAILY_EXPEDITURE_REQUESTS_STREAM_ID,
                DailyExpenditureRequest.getSchema());
        outputFieldsDeclarer
                .declareStream(TopologyControl.TRAVEL_TIME_REQUEST_STREAM_ID, TravelTimeRequest.getSchema());
    }

}
