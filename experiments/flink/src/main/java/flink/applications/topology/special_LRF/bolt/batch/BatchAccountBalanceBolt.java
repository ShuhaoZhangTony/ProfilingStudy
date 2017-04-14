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
import backtype.storm.tuple.Tuple;
import flink.applications.hooks.BoltMeterHook;
import flink.applications.topology.special_LRF.TopologyControl;
import flink.applications.topology.special_LRF.bolt.TollNotificationBolt;
import flink.applications.topology.special_LRF.model.AccountBalance;
import flink.applications.topology.special_LRF.model.VehicleAccount;
import flink.applications.topology.special_LRF.tools.Helper;
import flink.applications.topology.special_LRF.types.AccountBalanceRequest;
import flink.applications.topology.special_LRF.types.PositionReport;
import flink.applications.topology.special_LRF.types.TollNotification;
import flink.applications.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static flink.applications.util.config.Configuration.METRICS_ENABLED;


/**
 * This bolt recieves the toll values assesed by the {@link TollNotificationBolt} and answers to account balance
 * queries. Therefore it processes the streams {@link TopologyControl#TOLL_ASSESSMENTS_STREAM_ID} (for the former) and
 * {@link TopologyControl#ACCOUNT_BALANCE_REQUESTS_STREAM_ID} (for the latter)
 */
public class BatchAccountBalanceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(BatchAccountBalanceBolt.class);
    private final PositionReport inputPositionReport = new PositionReport();
    private OutputCollector collector;
    /**
     * Contains all vehicles and the accountinformation of the current day.
     */
    private Map<Integer, VehicleAccount> allVehicles;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.allVehicles = new HashMap<Integer, VehicleAccount>();
        Configuration config = Configuration.fromMap(conf);
        if (config.getBoolean(METRICS_ENABLED, false)) {
            context.addTaskHook(new BoltMeterHook());
        }
    }

    @Override
    public synchronized void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals(TopologyControl.ACCOUNT_BALANCE_REQUESTS_STREAM_ID)) {
            this.getBalanceAndSend(tuple);
        } else if (tuple.getSourceStreamId().equals(TopologyControl.TOLL_ASSESSMENTS_STREAM_ID)) {
            this.updateBalance(tuple);
        } else {
            throw new RuntimeException(String.format("Errornous stream subscription. Please report a bug at %s",
                    Helper.ISSUE_REPORT_URL));
        }

        this.collector.ack(tuple);
    }

    private synchronized void updateBalance(Tuple tuple) {

        //get input:
//        LinkedList<TollNotification> input_l = new LinkedList<>();
//        try {
//            if (tuple.getValue(0) instanceof LinkedList) {
//                input_l = (LinkedList<TollNotification>) tuple.getValue(0);
//            } else {
//                input_l.add((TollNotification) tuple.getValues());
//            }
//        } catch (Exception e) {
//            System.out.print(tuple.getValue(0) + " " + e.getMessage());
//        }


        LinkedList<TollNotification> input_l = (LinkedList<TollNotification>) tuple.getValue(0);
        for (int i = 0; i < input_l.size(); i++) {

            //Integer vid = input_l.get(i).getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME);
            Integer vid = input_l.get(i).getVid();

            VehicleAccount account = this.allVehicles.get(vid);

            //PositionReport pos = (PositionReport) tuple.getValueByField(TopologyControl.POS_REPORT_FIELD_NAME);
            PositionReport pos = input_l.get(i).getPos();

            if (account == null) {
                int assessedToll = 0;
                //TODO:assume it's 0 now.
                account = new VehicleAccount(assessedToll, pos.getVid(), pos.getXWay(), Long.valueOf(pos.getTime()));
                this.allVehicles.put(vid, account);
            } else {
                account.updateToll(input_l.get(i).getToll());
            }
        }
    }

    //    private synchronized void getBalanceAndSend(Tuple tuple) {
//
//
//        //get input:
//        LinkedList<Field_AccountBalanceRequest> input_l = (LinkedList<Field_AccountBalanceRequest>) tuple.getValue(0);
//        //output:
//        //       LinkedList<AccountBalance> output_l = new LinkedList<>();
//
//        for (int i = 0; i < input_l.size(); i++) {
//
//            Field_AccountBalanceRequest bal = input_l.get(i);
//
//            VehicleAccount account = this.allVehicles.get(bal.getVid());
//
//            if (account == null) {
//                LOG.debug("No account information available yet: at:" + bal.getTime() + " for request" + bal);
//                AccountBalance accountBalance = new AccountBalance
//                        (
//                                bal.getTime(),
//                                bal.getQid(),
//                                0, // balance
//                                0, // tollTime
//                                bal.getTime()
//                        );
//
//                this.collector.emit(TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, accountBalance);
//                //output_l.add(accountBalance);
//
//            } else {
//                AccountBalance accountBalance
//                        = account.getAccBalanceNotification(bal);
//                this.collector.emit(TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, accountBalance);
//                //output_l.add(accountBalance);
//            }
//        }
//        //       this.collector.emit(TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, new Values(output_l));
//    }
    private synchronized void getBalanceAndSend(Tuple tuple) {
        AccountBalanceRequest bal = new AccountBalanceRequest(
                tuple.getShortByField(TopologyControl.TIME_FIELD_NAME),
                tuple.getIntegerByField(TopologyControl.VEHICLE_ID_FIELD_NAME),
                tuple.getIntegerByField(TopologyControl.QUERY_ID_FIELD_NAME));

        VehicleAccount account = this.allVehicles.get(bal.getVid());

        if (account == null) {
            LOG.debug("No account information available yet: at:" + bal.getTime() + " for request" + bal);
            AccountBalance accountBalance = new AccountBalance
                    (
                            bal.getTime(),
                            bal.getQid(),
                            0, // balance
                            0, // tollTime
                            bal.getTime()
                    );
            this.collector.emit(TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, accountBalance);
        } else {
            AccountBalance accountBalance
                    = account.getAccBalanceNotification(bal);
            this.collector.emit(TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, accountBalance);
        }
    }

    public Map<Integer, VehicleAccount> getAllVehicles() {
        return this.allVehicles;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(
                TopologyControl.ACCOUNT_BALANCE_OUTPUT_STREAM_ID, AccountBalance.getSchema()
        );
    }

}
