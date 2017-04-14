package flink.applications.model.sentiment;

import flink.applications.util.config.Configuration;

/**
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize(Configuration config);

    public SentimentResult classify(String str);
}
