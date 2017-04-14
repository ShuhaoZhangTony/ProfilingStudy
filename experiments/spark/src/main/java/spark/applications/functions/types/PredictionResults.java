package spark.applications.functions.types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import spark.applications.model.fraud.predictor.MarkovModelPredictor;
import spark.applications.model.fraud.predictor.ModelBasedPredictor;

/**
 * Save the current status of the Fraud Detection Algorithm together with the currently detected outliers.
 * @author D050992
 *
 */

public class PredictionResults implements Serializable {
		

	private static final long serialVersionUID = -7897190882204225701L;
	
	private ModelBasedPredictor predictor;
	private List<Outlier> outlier;

	public PredictionResults(PredictionConfig config) {
		predictor = new MarkovModelPredictor(config);
		outlier = new ArrayList<Outlier>();
	}
	
	public void addOutlier(Outlier res) {
		this.outlier.add(res);
	}
	
	public void clearOutlier() {
		this.outlier.clear();
	}
	
	public ModelBasedPredictor getPredictor() {
		return predictor;
	}
	
	public List<Outlier> getOutlier() {
		return outlier;
	}
	
}
