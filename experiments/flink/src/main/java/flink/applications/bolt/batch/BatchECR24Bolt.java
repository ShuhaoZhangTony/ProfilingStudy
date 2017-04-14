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
 * Per-user received call rate.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchECR24Bolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchECR24Bolt.class);

    public BatchECR24Bolt(String configPrefix) {
        super(configPrefix, Field.RATE, null);
    }

    @Override
    public void execute(Tuple input) {

        //input..
        LinkedList<Values> batch_input = (LinkedList<Values>) input.getValue(0);


        //LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(4);
        int batch = batch_input.size();
        //output..
        LinkedList<String> batch_caller = new LinkedList<>();
        LinkedList<Long> batch_timestamp = new LinkedList<>();
        LinkedList<Double> batch_ecr = new LinkedList<>();
        LinkedList<CallDetailRecord> batch_cdr_output = new LinkedList<>();

        for (int i = 0; i < batch; i++) {
            CallDetailRecord cdr = (CallDetailRecord) batch_input.get(i).get(4);

            if (cdr.isCallEstablished()) {
                String caller = cdr.getCallingNumber();
                long timestamp = cdr.getAnswerTime().getMillis() / 1000;

                // add numbers to filters
                filter.add(caller, 1, timestamp);
                double ecr = filter.estimateCount(caller, timestamp);
                batch_caller.add(caller);
                batch_timestamp.add(timestamp);
                batch_ecr.add(ecr);
                batch_cdr_output.add(cdr);
            }
        }
        if (batch_caller.size() > 1) {
            collector.emit(new Values(batch_caller, batch_timestamp, batch_ecr, batch_cdr_output));//only build those iscallestablished
        }
        collector.ack(input);
    }
}
