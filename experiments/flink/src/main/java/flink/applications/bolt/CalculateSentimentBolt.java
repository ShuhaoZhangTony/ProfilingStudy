package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.SentimentAnalysisConstants.Conf;
import flink.applications.constants.SentimentAnalysisConstants.Field;
import flink.applications.model.sentiment.SentimentClassifier;
import flink.applications.model.sentiment.SentimentClassifierFactory;
import flink.applications.model.sentiment.SentimentResult;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

/**
 * Breaks each tweet into words and calculates the sentiment of each tweet and associates the sentiment value to the State
 * and logs the same to the console and also logs to the file.
 * https://github.com/voltas/real-time-sentiment-analytic
 *
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class CalculateSentimentBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateSentimentBolt.class);

    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
            .withLocale(Locale.ENGLISH);

    private SentimentClassifier classifier;

    @Override
    public void initialize() {
        String classifierType = config.getString(Conf.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.TEXT, Field.TIMESTAMP, Field.SENTIMENT, Field.SCORE);
    }

    @Override
    public void execute(Tuple input) {
        Map tweet = (Map) input.getValueByField(Field.TWEET);
        String tweetId = null;
        DateTime timestamp = null;
        if (tweet.containsKey("text")) {

            if (!tweet.containsKey("id_str")) {
                tweetId = "0000000000000000";
            } else {
                tweetId = (String) tweet.get("id_str");
            }
            if (!tweet.containsKey("created_at")) {
                timestamp = datetimeFormatter.parseDateTime("2013-12-06T15:00:00.000+08:00");
            } else {
                timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));
            }

            String text = (String) tweet.get("text");

            SentimentResult result = classifier.classify(text);

            collector.emit(input, new Values(tweetId, text, timestamp, result.getSentiment().toString(), result.getScore()));
        }
        collector.ack(input);
    }
}