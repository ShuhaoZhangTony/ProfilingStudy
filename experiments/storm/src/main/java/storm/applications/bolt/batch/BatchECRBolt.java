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
 * Per-user received call rate.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchECRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchECRBolt.class);
    private Multi_Key_value_Map cache;
    private int worker;

    public BatchECRBolt(String configPrefix) {
        super(configPrefix, Field.RATE, Field.RATE_KEY);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(outputField, outputkeyField);
    }


    @Override
    public void initialize() {
        super.initialize();
        worker=config.getInt(VoIPSTREAMConstants.Conf.FOFIR_THREADS, 1);
    }

    @Override
    public void execute(Tuple input) {

        //input..
        LinkedList<Values> batch_input = (LinkedList<Values>) input.getValue(0);
        cache = new Multi_Key_value_Map();

        //LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(4);
        int batch = batch_input.size();
        //output..
//        LinkedList<String> batch_caller = new LinkedList<>();
//        LinkedList<Long> batch_timestamp = new LinkedList<>();
//        LinkedList<Double> batch_ecr = new LinkedList<>();
//        LinkedList<CallDetailRecord> batch_cdr_output = new LinkedList<>();

        for (int i = 0; i < batch; i++) {
            CallDetailRecord cdr = (CallDetailRecord) batch_input.get(i).get(4);

            if (cdr.isCallEstablished()) {
                String caller = cdr.getCallingNumber();
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                // add numbers to filters
                filter.add(caller, 1, timestamp);
                double ecr = filter.estimateCount(caller, timestamp);
//                batch_caller.add(caller);
//                batch_timestamp.add(timestamp);
//                batch_ecr.add(ecr);
//                batch_cdr_output.add(cdr);
                Values v = new Values(caller, timestamp, ecr, cdr);
                cache.put(worker, v, caller);

            }
        }
//        if (batch_caller.size() > 1) {
//            collector.emit(new Values(batch_caller, batch_timestamp, batch_ecr, batch_cdr_output));//only build those iscallestablished
//        }
        cache.emit(VoIPSTREAMConstants.Stream.DEFAULT,this.collector);
        collector.ack(input);
    }
}
