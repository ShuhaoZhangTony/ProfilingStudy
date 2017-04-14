package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.model.cdr.CallDetailRecord;
import storm.applications.util.Multi_Key_value_Map;
import storm.applications.util.bloom.BloomFilter;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static storm.applications.constants.VoIPSTREAMConstants.*;

public class BatchVariationDetectorBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchVariationDetectorBolt.class);

    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;
    private Multi_Key_value_Map cache;
    private int worker;
    private Multi_Key_value_Map cache_by_called;

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();

        Fields fields = new Fields(Field.RECORD, Field.VARIATION_DETECTOR_Split_Key);

        streams.put(Stream.DEFAULT, fields);
        streams.put(Stream.BACKUP, fields);

        return streams;
    }

    @Override
    public void initialize() {
        approxInsertSize = config.getInt(Conf.VAR_DETECT_APROX_SIZE);
        falsePostiveRate = config.getDouble(Conf.VAR_DETECT_ERROR_RATE);

        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner = new BloomFilter<>(falsePostiveRate, approxInsertSize);

        cycleThreshold = detector.size() / Math.sqrt(2);
        worker = Math.max(config.getInt(Conf.ECR_THREADS, 1), config.getInt(Conf.RCR_THREADS, 1));
    }

    @Override
    public void execute(Tuple input) {

        //input..
        LinkedList<CallDetailRecord> batch_cdr = (LinkedList<CallDetailRecord>) input.getValue(0);
        int batch = batch_cdr.size();
        cache = new Multi_Key_value_Map();
        cache_by_called = new Multi_Key_value_Map();
        //output..
//        String[] CALLING_NO = new String[batch];
//        String[] CALLED_NO = new String[batch];
//        DateTime[] AnswerTime = new DateTime[batch];
//        boolean[] batchNewCallee = new boolean[batch];

        for (int i = 0; i < batch; i++) {
            CallDetailRecord cdr = batch_cdr.get(i);
            String key = String.format("%s:%s", cdr.getCallingNumber(), cdr.getCalledNumber());
            boolean newCallee = false;

            // add pair to learner
            learner.add(key);

            if (!detector.membershipTest(key)) {
                detector.add(key);
                newCallee = true;
            }

            // if number of non-zero bits is above threshold, rotate filters
            if (detector.getNumNonZero() > cycleThreshold) {
                rotateFilters();
            }

            //Values values = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(),
            //        cdr.getAnswerTime(), newCallee, cdr);
            //batch_values[i]=values;
//            CALLING_NO[i] = cdr.getCallingNumber();
//            CALLED_NO[i] = cdr.getCalledNumber();
//            AnswerTime[i] = cdr.getAnswerTime();
//            batchNewCallee[i] = newCallee;

            Values v = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), newCallee, cdr);
            cache.put(worker, v, cdr.getCallingNumber());
            cache_by_called.put(worker, v, cdr.getCalledNumber());
        }

        cache.emit(Stream.DEFAULT, this.collector);
        cache_by_called.emit(Stream.BACKUP, this.collector);
//        collector.emit(values);
//        collector.emit(Stream.BACKUP, values);
        collector.ack(input);
    }

    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}