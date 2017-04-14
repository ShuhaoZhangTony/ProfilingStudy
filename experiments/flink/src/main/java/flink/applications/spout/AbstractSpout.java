package flink.applications.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import flink.applications.constants.BaseConstants;
import flink.applications.constants.BaseConstants.BaseStream;
import flink.applications.hooks.SpoutMeterHook;
import flink.applications.util.config.Configuration;

import java.util.HashMap;
import java.util.Map;

import static flink.applications.util.config.Configuration.METRICS_ENABLED;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractSpout extends BaseRichSpout {
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    protected Configuration config;
    protected SpoutOutputCollector collector;
    protected TopologyContext context;
    protected Map<String, Fields> fields;

    public AbstractSpout() {
        fields = new HashMap<>();
    }

    public void setFields(Fields fields) {
        this.fields.put(BaseStream.DEFAULT, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.config = Configuration.fromMap(conf);
        this.collector = collector;
        this.context = context;

        if (config.getBoolean(METRICS_ENABLED, false)) {
            context.addTaskHook(new SpoutMeterHook());
        }

        initialize();
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    protected abstract void initialize();
}
