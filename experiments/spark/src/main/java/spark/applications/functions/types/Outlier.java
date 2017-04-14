package spark.applications.functions.types;

import java.io.Serializable;


/**
 * Outlier detected by the Fraud Detection Algorithm.
 * @author Thomas Heinze
 *
 */
public class Outlier implements Serializable {

	
	private static final long serialVersionUID = -6544591790134606231L;
	
	private String entityID;
	private double score;
	private String state;

	public Outlier(String entityID, double score, String state) {
		this.entityID = entityID;
		this.score = score;
	
		this.state = state;
	}

	public String getEntityID() {
		return entityID;
	}

	public double getScore() {
		return score;
	}

	public String getState() {
		return state;
	}
	
	public String toString() {
		return entityID +", " + score + ", "+ state;
	}
}
