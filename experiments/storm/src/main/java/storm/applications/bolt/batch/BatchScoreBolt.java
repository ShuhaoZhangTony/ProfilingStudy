package storm.applications.bolt.batch;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractScoreBolt;
import storm.applications.model.cdr.CallDetailRecord;

import java.util.LinkedList;

import static storm.applications.constants.VoIPSTREAMConstants.Conf;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchScoreBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchScoreBolt.class);

    private double[] weights;

    public BatchScoreBolt() {
        super(null);
    }

    /**
     * Computes weighted sum of a given sequence.
     *
     * @param data    data array
     * @param weights weights
     * @return weighted sum of the data
     */
    private static double sum(double[] data, double[] weights) {
        double sum = 0.0;

        for (int i = 0; i < data.length; i++) {
            sum += (data[i] * weights[i]);
        }

        return sum;
    }

    @Override
    public void initialize() {
        super.initialize();

        // parameters
        double fofirWeight = config.getDouble(Conf.FOFIR_WEIGHT);
        double urlWeight = config.getDouble(Conf.URL_WEIGHT);
        double acdWeight = config.getDouble(Conf.ACD_WEIGHT);

        weights = new double[3];
        weights[0] = fofirWeight;
        weights[1] = urlWeight;
        weights[2] = acdWeight;
    }

    @Override
    public void execute(Tuple input) {
        //input..
        Source src = parseComponentId(input.getSourceComponent());
        LinkedList<Double> batch_score = (LinkedList<Double>) input.getValue(2);
        LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(3);
        int batch = batch_cdr.size();

        for (int i = 0; i < batch; i++) {
            CallDetailRecord cdr = batch_cdr.get(i);
            String caller = cdr.getCallingNumber();
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;
            double score = batch_score.get(i);
            String key = String.format("%s:%d", caller, timestamp);

            if (map.containsKey(key)) {
                Entry e = map.get(key);

                if (e.isFull()) {
                    double mainScore = sum(e.getValues(), weights);

                    //LOG.debug(String.format("Score=%f; Scores=%s", mainScore, Arrays.toString(e.getValues())));

                    collector.emit(new Values(caller, timestamp, mainScore, cdr));
                } else {
                    e.set(src, score);
                    //    collector.emit(new Values(caller, timestamp, 0, cdr));
                }
            } else {
                Entry e = new Entry(cdr);
                e.set(src, score);
                map.put(key, e);
                collector.emit(new Values(caller, timestamp, 0, cdr));
            }
        }
        collector.ack(input);
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.FOFIR, Source.URL, Source.ACD};
    }
}
