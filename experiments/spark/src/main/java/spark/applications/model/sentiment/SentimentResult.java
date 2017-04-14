package spark.applications.model.sentiment;

/**
 * @author mayconbordin
 */
public class SentimentResult {
    private Sentiment sentiment;
    private double score;

    public SentimentResult() {
    }

    public SentimentResult(Sentiment sentiment) {
        this.sentiment = sentiment;
    }

    public SentimentResult(Sentiment sentiment, int score) {
        this.sentiment = sentiment;
        this.score = score;
    }

    public Sentiment getSentiment() {
        return sentiment;
    }

    public void setSentiment(Sentiment sentiment) {
        this.sentiment = sentiment;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public static enum Sentiment {
        Positive, Negative, Neutral
    }
}
