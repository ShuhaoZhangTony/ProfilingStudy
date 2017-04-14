package spark.applications.functions.types;

import java.io.Serializable;

import org.apache.spark.SparkConf;

import spark.applications.constants.FraudDetectionConstants.Conf;

/**
 * Extract Fraud prediction related configuration parameters from Spark Config to create a serializable object.
 * @author Thomas Heinze
 *
 */
public class PredictionConfig implements Serializable{

	private static final long serialVersionUID = -9184312186418914881L;

	private String modelKey;
	private boolean localPredictor;
	private int stateSeqWindowSize;
	private int stateOrdinal;
	private String algorithm;
	private double metricThreshold;

	public PredictionConfig(SparkConf conf) {
		modelKey = conf.get(Conf.MARKOV_MODEL_KEY, null);
		localPredictor = conf.getBoolean(Conf.LOCAL_PREDICTOR, true);
		stateSeqWindowSize = conf.getInt(Conf.STATE_SEQ_WIN_SIZE, 0);
		stateOrdinal = conf.getInt(Conf.STATE_ORDINAL, 0);

		// detection algoritm
		algorithm = conf.get(Conf.DETECTION_ALGO);
		metricThreshold = conf.getDouble(Conf.METRIC_THRESHOLD, 0.0d);
	}

	public String getModelKey() {
		return modelKey;
	}

	public boolean isLocalPredictor() {
		return localPredictor;
	}

	public int getStateSeqWindowSize() {
		return stateSeqWindowSize;
	}

	public int getStateOrdinal() {
		return stateOrdinal;
	}

	public String getAlgorithm() {
		return algorithm;
	}

	public double getMetricThreshold() {
		return metricThreshold;
	}
}
