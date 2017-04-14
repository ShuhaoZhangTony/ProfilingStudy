package storm.applications.spout;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.parser.Parser;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.stream.StreamValues;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This is an encapsulation of the storm-kafka-0.8-plus spout implementation.
 * It only emits tuples to the default stream, also only the first value of the parser
 * is going to be emitted, this is a limitation of the implementation, most applications
 * won't be affected.
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class KafkaSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);
    private static org.apache.storm.kafka.KafkaSpout spout;
    private BrokerHosts brokerHosts;

    @Override
    protected void initialize() {
        String parserClass = config.getString(getConfigKey(BaseConf.SPOUT_PARSER));
        String host = config.getString(getConfigKey(BaseConf.KAFKA_HOST));
        String topic = config.getString(getConfigKey(BaseConf.KAFKA_SPOUT_TOPIC));
        String consumerId = config.getString(getConfigKey(BaseConf.KAFKA_CONSUMER_ID));
        String path = config.getString(getConfigKey(BaseConf.KAFKA_ZOOKEEPER_PATH));

        Parser parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);

        Fields defaultFields = fields.get(Utils.DEFAULT_STREAM_ID);
        if (defaultFields == null) {
            throw new RuntimeException("A KafkaSpout must have a default stream");
        }

        brokerHosts = new ZkHosts(host);

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, path, consumerId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new ParserScheme(parser, defaultFields));

        spout = new org.apache.storm.kafka.KafkaSpout(spoutConfig);
        spout.open(config, context, collector);
    }

    @Override
    public void nextTuple() {
        spout.nextTuple();
    }

    @Override
    public void fail(Object msgId) {
        spout.fail(msgId);
    }

    @Override
    public void ack(Object msgId) {
        spout.ack(msgId);
    }

    @Override
    public void deactivate() {
        spout.deactivate();
    }

    @Override
    public void close() {
        spout.close();
    }

    private class ParserScheme extends StringScheme {
        private final Parser parser;
        private final Fields fields;

        public ParserScheme(Parser parser, Fields fields) {
            this.parser = parser;
            this.fields = fields;
        }

        @Override
        public List<Object> deserialize(ByteBuffer bytes) {
            String value = deserializeString(bytes);

            List<StreamValues> tuples = parser.parse(value);

            if (tuples != null && tuples.size() > 0) {
                return tuples.get(0);
            }

            return null;
        }

        @Override
        public Fields getOutputFields() {
            return fields;
        }
    }
}
