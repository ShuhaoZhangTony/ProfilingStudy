package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractFilterBolt;
import flink.applications.constants.VoIPSTREAMConstants;
import flink.applications.model.cdr.CallDetailRecord;
import flink.applications.util.Multi_Key_value_Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static flink.applications.constants.VoIPSTREAMConstants.Field;
import static flink.applications.constants.VoIPSTREAMConstants.Stream;

/**
 * Per-user received call rate.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchRCRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchRCRBolt.class);
    private Multi_Key_value_Map cache;
    private int worker;

    public BatchRCRBolt() {
        super("rcr", Field.RATE, Field.RATE_KEY);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(outputField, outputkeyField);
    }

    @Override
    public void initialize() {
        super.initialize();
        worker = config.getInt(VoIPSTREAMConstants.Conf.FOFIR_THREADS, 1);
    }

    @Override
    public void execute(Tuple input) {

        //input..
        LinkedList<Values> batch_input = (LinkedList<Values>) input.getValue(0);
        cache = new Multi_Key_value_Map();

        //LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(0);
        int batch = batch_input.size();

        //output...
//        LinkedList<String> batch_caller = new LinkedList<>();
//        LinkedList<Long> batch_timestamp = new LinkedList<>();
//        LinkedList<Double> batch_rcr = new LinkedList<>();
//        LinkedList<CallDetailRecord> batch_cdr_output = new LinkedList<>();

        for (int i = 0; i < batch; i++) {

            CallDetailRecord cdr = (CallDetailRecord) batch_input.get(i).get(4);

            if (cdr.isCallEstablished()) {
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                if (input.getSourceStreamId().equals(Stream.DEFAULT)) {
                    String callee = cdr.getCalledNumber();
                    filter.add(callee, 1, timestamp);
                } else if (input.getSourceStreamId().equals(Stream.BACKUP)) {
                    String caller = cdr.getCallingNumber();
                    double rcr = filter.estimateCount(caller, timestamp);
//                    batch_caller.add(caller);
//                    batch_timestamp.add(timestamp);
//                    batch_rcr.add(rcr);
//                    batch_cdr_output.add(cdr);
//                    collector.emit(new Values(caller, timestamp, rcr, cdr));


                    Values v = new Values(caller, timestamp, rcr, cdr);
                    cache.put(worker, v, caller);
                }
            }
        }
//        if (batch_caller.size() > 1) {
//            collector.emit(new Values(batch_caller, batch_timestamp, batch_rcr, batch_cdr_output));
//        }
        cache.emit(VoIPSTREAMConstants.Stream.DEFAULT, this.collector);
        collector.ack(input);
    }
}
