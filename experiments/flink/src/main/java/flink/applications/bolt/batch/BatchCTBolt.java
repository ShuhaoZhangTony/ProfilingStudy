package flink.applications.bolt.batch;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractFilterBolt;
import flink.applications.model.cdr.CallDetailRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static flink.applications.constants.VoIPSTREAMConstants.Field;

/**
 * Per-user total call time
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchCTBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchCTBolt.class);

    public BatchCTBolt(String configPrefix) {
        super(configPrefix, Field.CALLTIME, null);
    }

    @Override
    public void execute(Tuple input) {
        //input..
        LinkedList<Values> batch_input = (LinkedList<Values>) input.getValue(0);


        //String[] CALLING_NO = (String[]) input.getValue(0);
        //boolean[] batchNewCallee = (boolean[]) input.getValue(3);
        //LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(4);
        int batch = batch_input.size();

        //output..
        LinkedList<String> batch_caller = new LinkedList<>();
        LinkedList<Long> batch_timestamp = new LinkedList<>();
        LinkedList<Double> batch_calltime = new LinkedList<>();
        LinkedList<CallDetailRecord> batch_cdr_output = new LinkedList<>();

        for (int i = 0; i < batch; i++) {
            Values v = batch_input.get(i);
            CallDetailRecord cdr = (CallDetailRecord) v.get(4);
            boolean newCallee = (boolean) v.get(3);

            if (cdr.isCallEstablished() && newCallee) {
                String caller = (String) v.get(0);
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                filter.add(caller, cdr.getCallDuration(), timestamp);
                double calltime = filter.estimateCount(caller, timestamp);

//                LOG.debug(String.format("CallTime: %f", calltime));
//                collector.emit(new Values(caller, timestamp, calltime, cdr));
                batch_caller.add(caller);
                batch_timestamp.add(timestamp);
                batch_calltime.add(calltime);
                batch_cdr_output.add(cdr);
            }
        }
        if (batch_caller.size() > 1)
            collector.emit(new Values(batch_caller, batch_timestamp, batch_calltime, batch_cdr_output));
        collector.ack(input);
    }
}