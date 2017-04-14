package spark.applications.model.sentiment;

import spark.applications.util.config.Configuration;

/**
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize(Configuration config);

    public SentimentResult classify(String str);
}
