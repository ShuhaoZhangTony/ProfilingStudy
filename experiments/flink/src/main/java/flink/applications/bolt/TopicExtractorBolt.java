package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.TrendingTopicsConstants.Field;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.StringTokenizer;

/**
 * @author mayconbordin
 */
public class TopicExtractorBolt extends AbstractBolt {
    @Override
    public void execute(Tuple input) {
        Map tweet = (Map) input.getValueByField(Field.TWEET);
        String text = (String) tweet.get("text");

        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    collector.emit(input, new Values(term));
                }
            }
        }

        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }
}
