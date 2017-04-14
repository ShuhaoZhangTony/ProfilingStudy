package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.model.fraud.predictor.MarkovModelPredictor;
import flink.applications.model.fraud.predictor.ModelBasedPredictor;
import flink.applications.model.fraud.predictor.Prediction;
import org.apache.commons.lang.StringUtils;

import static flink.applications.constants.FraudDetectionConstants.Conf;
import static flink.applications.constants.FraudDetectionConstants.Field;

/**
 * @author maycon
 */
public class FraudPredictorBolt extends AbstractBolt {
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

        String entityID = input.getString(0);
        String record = input.getString(1);
        Prediction p = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier()) {
            //e_index++;
            //if(e_index%100==0)
            collector.emit(input, new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
        }

        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ENTITY_ID, Field.SCORE, Field.STATES);
    }
}