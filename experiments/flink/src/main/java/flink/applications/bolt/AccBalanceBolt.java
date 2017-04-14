package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.util.event.AccountBalanceEvent;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Created by szhang026 on 21/2/2016.
 */
public class AccBalanceBolt extends flink.applications.bolt.base.AbstractBolt {
    private int count;
    private LinkedList<AccountBalanceEvent> accEvtList;
    private HashMap<Integer, Integer> tollList;

    public AccBalanceBolt(int count) {
        this.count = count;
        setFields("accbalance_event", new Fields("vid",
                "toll"));
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);
    }
//    @Override
//    public Fields getDefaultFields() {
//
//
//        return new Fields("vid",
//                "toll");
//    }

//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream("accbalance_event", new Fields("vid",
//                "toll"));
//    }

    public void process(AccountBalanceEvent evt) {
        //int len = 0;
        //Statement stmt;
        //BytesMessage bytesMessage = null;

        if (evt != null) {
//				try{
//				    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
//				    bytesMessage.writeBytes((Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid)).getBytes());
//				    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//				    producer_output.send(bytesMessage);
//				}catch(JMSException e){
//					e.printStackTrace();
//				}

            collector.emit("accbalance_event", new Values(evt.vid, tollList.get(evt.vid)));
        }
    }
}
