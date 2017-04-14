package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.math.VariableEWMA;

import java.util.LinkedList;

import static storm.applications.constants.VoIPSTREAMConstants.Conf;
import static storm.applications.constants.VoIPSTREAMConstants.Field;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchGlobalACDBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchGlobalACDBolt.class);

    private VariableEWMA avgCallDuration;
    private double decayFactor;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.AVERAGE);
    }

    @Override
    public void initialize() {
        decayFactor = config.getDouble(Conf.ACD_DECAY_FACTOR, 86400); //86400s = 24h
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public void execute(Tuple input) {

        //input..
//        LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(4);
        LinkedList<Values> batch_input = (LinkedList<Values>) input.getValue(0);
        int batch = batch_input.size();

        //output...
        LinkedList<Long> batch_timestamp = new LinkedList<>();
        LinkedList<Double> average = new LinkedList<>();

        for (int i = 0; i < batch; i++) {
            CallDetailRecord cdr = (CallDetailRecord) batch_input.get(i).get(4);
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            avgCallDuration.add(cdr.getCallDuration());

            batch_timestamp.add(timestamp);
            average.add(avgCallDuration.getAverage());

        }

        collector.emit(new Values(batch_timestamp, average));
        collector.ack(input);
    }
}