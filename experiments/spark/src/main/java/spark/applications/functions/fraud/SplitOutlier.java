package spark.applications.functions.fraud;

import org.apache.spark.api.java.function.Function;

import spark.applications.functions.types.PredictionResults;
import spark.applications.functions.types.Outlier;

public class SplitOutlier implements Function<PredictionResults, Iterable<Outlier>> {

	private static final long serialVersionUID = 6620181156523489292L;

	@Override
	public Iterable<Outlier> call(PredictionResults predRes) throws Exception {
		
		return predRes.getOutlier();
	}

}
