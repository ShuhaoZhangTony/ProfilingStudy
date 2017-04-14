package storm.applications.bolt.batch;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractScoreBolt;
import storm.applications.model.cdr.CallDetailRecord;

import java.util.LinkedList;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BatchACDBolt extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchACDBolt.class);

    private LinkedList<Double> avg;
    private int batch = Integer.MAX_VALUE;

    public BatchACDBolt() {
        super("acd");
    }

    @Override
    public void execute(Tuple input) {

        Source src = parseComponentId(input.getSourceComponent());
        if (src == Source.GACD) {
            //input...
            avg = (LinkedList<Double>) input.getValue(1);
            batch = Math.min(avg.size(), batch);
            //no output...
        } else {

            //input..
            LinkedList<String> batch_number = (LinkedList<String>) input.getValue(0);
            LinkedList<Long> batch_timestamp = (LinkedList<Long>) input.getValue(1);
            LinkedList<Double> batch_rate = (LinkedList<Double>) input.getValue(2);
            LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(3);

            //output..
            LinkedList<String> batch_number_output = new LinkedList<>();
            LinkedList<Long> batch_timestamp_output = new LinkedList<>();
            LinkedList<Double> batch_score_output = new LinkedList<>();
            LinkedList<CallDetailRecord> batch_cdr_output = new LinkedList<>();

            batch = Math.min(batch_cdr.size(), batch);
            for (int i = 0; i < batch; i++) {
                CallDetailRecord cdr = batch_cdr.get(i);
                String number = batch_number.get(i);
                long timestamp = batch_timestamp.get(i);
                double rate = batch_rate.get(i);

                String key = String.format("%s:%d", number, timestamp);

                if (map.containsKey(key)) {
                    Entry e = map.get(key);
                    e.set(src, rate);

                    if (e.isFull()) {
                        // calculate the score for the ratio
                        try {
                            double ratio = (e.get(Source.CT24) / e.get(Source.ECR24)) / avg.get(i);
                            double score = score(thresholdMin, thresholdMax, ratio);

//                        LOG.debug(String.format("T1=%f; T2=%f; CT24=%f; ECR24=%f; AvgCallDur=%f; Ratio=%f; Score=%f",
//                                thresholdMin, thresholdMax, e.get(Source.CT24), e.get(Source.ECR24), avg, ratio, score));

//                        collector.emit(new Values(number, timestamp, score, cdr));
                            batch_number_output.add(number);
                            batch_timestamp_output.add(timestamp);
                            batch_score_output.add(score);
                            batch_cdr_output.add(cdr);

                            map.remove(key);
                        } catch (Exception ex) {
                            System.out.println("");
                        }
                    } else {
                        LOG.debug(String.format("Inconsistent entry: source=%s; %s",
                                input.getSourceComponent(), e.toString()));
                    }
                } else {
                    Entry e = new Entry(cdr);
                    e.set(src, rate);
                    map.put(key, e);
                }
            }
            if (batch_number_output.size() >= 1)
                collector.emit(new Values(batch_number_output, batch_timestamp_output, batch_score_output, batch_cdr_output));
        }
        collector.ack(input);
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.CT24, Source.ECR24};
    }
}