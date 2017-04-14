package spark.applications.functions.fraud;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.Function2;

import com.google.common.base.Optional;

import spark.applications.functions.types.Outlier;
import spark.applications.functions.types.PredictionConfig;
import spark.applications.functions.types.PredictionResults;
import spark.applications.model.fraud.predictor.Prediction;

/**
 * Created by szhang026 on 5/6/2016.
 */
public class FraudPredictor implements Function2<List<String>,  Optional<PredictionResults>, Optional<PredictionResults>> {
  
	private static final long serialVersionUID = -3442681684713996296L;
	
	private PredictionConfig config; 
	public FraudPredictor(PredictionConfig config) {
		this.config = config;
	}

    @Override
    public Optional<PredictionResults> call(List<String> values, Optional<PredictionResults> pred) throws Exception {
    		PredictionResults newPred =pred.or(new PredictionResults(config));
    		for(String v: values) {
    			String parts[] = v.split(",");
    			Prediction p =	newPred.getPredictor().execute(parts[0],parts[1]+","+ parts[2]);
    			if(p.isOutlier()) {
    				newPred.addOutlier(new Outlier(parts[0], p.getScore(), StringUtils.join(p.getStates(),",")));
    			}
    		}
    		return Optional.of(newPred);
    }


}
