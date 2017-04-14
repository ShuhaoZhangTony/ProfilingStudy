package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
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

public class BatchVariationDetectorSplitBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchVariationDetectorSplitBolt.class);

    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;
    private int batch;
    private Multi_Key_value_Map cache;
    private int worker;

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();

        Fields fields = new Fields(Field.RECORD, Field.VARIATION_DETECTOR_Split_Key);

        streams.put(Stream.DEFAULT, fields);
        return streams;
    }

    @Override
    public void initialize() {
        approxInsertSize = config.getInt(Conf.VAR_DETECT_APROX_SIZE);
        falsePostiveRate = config.getDouble(Conf.VAR_DETECT_ERROR_RATE);

        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner = new BloomFilter<>(falsePostiveRate, approxInsertSize);

        cycleThreshold = detector.size() / Math.sqrt(2);
        worker = config.getInt((Conf.VAR_DETECT_THREADS));
    }

    @Override
    public void execute(Tuple input) {
        String raw = null;
        LinkedList<CallDetailRecord> batchedInput = (LinkedList<CallDetailRecord>) input.getValue(0);
        batch = batchedInput.size();
        cache = new Multi_Key_value_Map();
        for (int i = 0; i < batch; i++) {
            CallDetailRecord v = batchedInput.get(i);
            cache.put(worker, v, v.getCallingNumber(), v.getCalledNumber());
        }
        //if (!cache.isEmpty())
        cache.emit(BaseStream.DEFAULT, this.collector);
        collector.ack(input);
    }
}