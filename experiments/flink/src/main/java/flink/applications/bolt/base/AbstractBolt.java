package flink.applications.bolt.base;

import backtype.storm.hooks.ITaskHook;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import flink.applications.constants.BaseConstants;
import flink.applications.constants.BaseConstants.BaseStream;
import flink.applications.hooks.BoltMeterHook;
import flink.applications.util.config.Configuration;

import java.util.HashMap;
import java.util.Map;

import static flink.applications.util.config.Configuration.METRICS_ENABLED;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractBolt extends BaseRichBolt {
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    protected final String splitregex = ",";
    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    protected Map<String, Fields> fields;

    public AbstractBolt() {
        fields = new HashMap<>();
    }

    public void setFields(Fields fields) {
        this.fields.put(BaseStream.DEFAULT, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (fields.isEmpty()) {
            if (getDefaultFields() != null)
                fields.put(BaseStream.DEFAULT, getDefaultFields());

            if (getDefaultStreamFields() != null)
                fields.putAll(getDefaultStreamFields());
        }

        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    public Fields getDefaultFields() {//@define the output fields
        return null;
    }

    public Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.config = Configuration.fromMap(stormConf);
        this.context = context;
        this.collector = collector;

        if (config.getBoolean(METRICS_ENABLED, false)) {
            context.addTaskHook(getMeterHook());
        }

        initialize();
    }

    public void initialize() {

    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    protected ITaskHook getMeterHook() {
        return new BoltMeterHook();
    }
}
