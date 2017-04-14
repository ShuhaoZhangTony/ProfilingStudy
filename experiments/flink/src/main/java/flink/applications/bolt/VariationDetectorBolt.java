package flink.applications.bolt;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.model.cdr.CallDetailRecord;
import flink.applications.util.bloom.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static flink.applications.constants.VoIPSTREAMConstants.*;

public class VariationDetectorBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VariationDetectorBolt.class);
    transient CountMetric _countMetric;
    transient MultiCountMetric _wordCountMetric;
    transient ReducedMetric _wordLengthMeanMetric;
    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();

        Fields fields = new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.NEW_CALLEE, Field.RECORD);

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

        //initMetrics(context);


    }

    void initMetrics(TopologyContext context) {
        _countMetric = new CountMetric();
        _wordCountMetric = new MultiCountMetric();
        _wordLengthMeanMetric = new ReducedMetric(new MeanReducer());

        context.registerMetric("execute_count", _countMetric, 5);
        context.registerMetric("word_count", _wordCountMetric, 60);
        context.registerMetric("word_length", _wordLengthMeanMetric, 60);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        String key = String.format("%s:%s", cdr.getCallingNumber(), cdr.getCalledNumber());
        boolean newCallee = false;

        // add pair to learner
        learner.add(key);

        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }

        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }

        Values values = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(),
                cdr.getAnswerTime(), newCallee, cdr);

        collector.emit(values);
        collector.emit(Stream.BACKUP, values);
        collector.ack(input);

        //updateMetrics();
    }

    void updateMetrics() {
        _countMetric.incr();
        //_wordCountMetric.scope(word).incr();
        //_wordLengthMeanMetric.update(word.length());
    }

    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}