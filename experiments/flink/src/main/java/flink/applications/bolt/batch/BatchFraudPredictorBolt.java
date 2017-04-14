package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.model.fraud.predictor.MarkovModelPredictor;
import flink.applications.model.fraud.predictor.ModelBasedPredictor;
import flink.applications.model.fraud.predictor.Prediction;
import org.apache.commons.lang.StringUtils;

import java.util.LinkedList;

import static flink.applications.constants.FraudDetectionConstants.Conf;
import static flink.applications.constants.FraudDetectionConstants.Field;

/**
 * @author maycon
 */
public class BatchFraudPredictorBolt extends AbstractBolt {
    private ModelBasedPredictor predictor;
    private int e_index = 0;

    @Override
    public void initialize() {
        String strategy = config.getString(Conf.PREDICTOR_MODEL);

        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public void execute(Tuple input) {
        LinkedList<Values> batch_input = (LinkedList<Values>) input.getValue(0);
        int batch = batch_input.size();
//
//        String[] batchedEntityID = ((String[]) input.getValue(0));
//        String[] batchedRecord = ((String[]) input.getValue(1));
        for (int i = 0; i < batch; i++) {
            Values v = batch_input.get(i);
            String entityID = (String) v.get(0);
            String record = (String) v.get(1);
            Prediction p = predictor.execute(entityID, record);

            // send outliers
            if (p.isOutlier()) {
                //e_index++;
                //if(e_index%100==0)
                collector.emit(input, new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
            }
        }
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ENTITY_ID, Field.SCORE, Field.STATES);
    }
}