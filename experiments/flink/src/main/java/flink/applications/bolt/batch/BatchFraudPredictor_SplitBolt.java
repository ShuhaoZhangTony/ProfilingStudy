package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.BaseConstants;
import flink.applications.model.fraud.predictor.MarkovModelPredictor;
import flink.applications.model.fraud.predictor.ModelBasedPredictor;
import flink.applications.util.Multi_Key_value_Map;

import java.util.LinkedList;

import static flink.applications.constants.FraudDetectionConstants.Conf;
import static flink.applications.constants.FraudDetectionConstants.Field;

/**
 * @author maycon
 */
public class BatchFraudPredictor_SplitBolt extends AbstractBolt {
    private ModelBasedPredictor predictor;
    private int e_index = 0;
    private Multi_Key_value_Map cache;
    private int worker;

    @Override
    public void initialize() {
        String strategy = config.getString(Conf.PREDICTOR_MODEL);

        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
        worker = config.getInt(Conf.PREDICTOR_THREADS, 1);
    }

    @Override
    public void execute(Tuple input) {

//        String[] batchedEntityID = ((String[]) input.getValue(0));
//        String[] batchedRecord = ((String[]) input.getValue(1));
        LinkedList<String> batch_input = (LinkedList<String>) input.getValue(0);
        int batch = batch_input.size();

        cache = new Multi_Key_value_Map();

        for (int i = 0; i < batch; i++) {
            String[] v = batch_input.get(i).split(",", 2);
            String entityID = v[0];
            String record = v[1];

            //collector.emit(input, new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
            cache.put(worker, new Values(entityID, record), entityID);
        }
        cache.emit(BaseConstants.BaseStream.DEFAULT, this.collector);
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.RECORD_DATA, Field.RECORD_KEY);
    }
}