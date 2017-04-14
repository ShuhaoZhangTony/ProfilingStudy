package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractFilterBolt;
import storm.applications.constants.VoIPSTREAMConstants;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.Multi_Key_value_Map;

import java.util.LinkedList;

import static storm.applications.constants.VoIPSTREAMConstants.Field;

/**
 * Per-user new callee rate
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchENCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchENCRBolt.class);
    private Multi_Key_value_Map cache;
    private int worker;

    public BatchENCRBolt() {
        super("encr", Field.RATE, Field.RATE_KEY);
    }

    @Override
    public void initialize() {
        super.initialize();
        worker = config.getInt(VoIPSTREAMConstants.Conf.URL_THREADS, 1);
    }
    @Override
    public Fields getDefaultFields() {
        return new Fields(outputField, outputkeyField);
    }
    @Override
    public void execute(Tuple input) {

        //input..
        LinkedList<Values> batch_input = (LinkedList<Values>) input.getValue(0);


        //String[] CALLING_NO = (String[]) input.getValue(0);
        //boolean[] batchNewCallee = (boolean[]) input.getValue(3);
        //LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(4);
        int batch = batch_input.size();
        cache = new Multi_Key_value_Map();
        //output..
//        LinkedList<String> batch_caller = new LinkedList<>();
//        LinkedList<Long> batch_timestamp = new LinkedList<>();
//        LinkedList<Double> batch_rate = new LinkedList<>();
//        LinkedList<CallDetailRecord> batch_cdr_output = new LinkedList<>();

        for (int i = 0; i < batch; i++) {
            Values v = batch_input.get(i);
            CallDetailRecord cdr = (CallDetailRecord) v.get(4);
            boolean newCallee = (boolean) v.get(3);

            if (cdr.isCallEstablished() && newCallee) {
                String caller = (String) v.get(0);
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                filter.add(caller, 1, timestamp);
                double rate = filter.estimateCount(caller, timestamp);
                //collector.emit(new Values(caller, timestamp, rate, cdr));
//                batch_caller.add(caller);
//                batch_timestamp.add(timestamp);
//                batch_rate.add(rate);
//                batch_cdr_output.add(cdr);
                Values ov = new Values(caller, timestamp, rate, cdr);
                cache.put(worker, ov, caller);
            }
        }
//        if (batch_caller.size() > 1) {
//            collector.emit(new Values(batch_caller, batch_timestamp, batch_rate, batch_cdr_output));
//        }
        cache.emit(VoIPSTREAMConstants.Stream.DEFAULT, this.collector);
        collector.ack(input);
    }
}