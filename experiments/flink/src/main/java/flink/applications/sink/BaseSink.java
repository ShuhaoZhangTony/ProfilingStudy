package flink.applications.sink;

import backtype.storm.tuple.Fields;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.BaseConstants.BaseConf;
import flink.applications.sink.formatter.BasicFormatter;
import flink.applications.sink.formatter.Formatter;
import flink.applications.util.config.ClassLoaderUtils;
import org.slf4j.Logger;


public abstract class BaseSink extends AbstractBolt {
    protected Formatter formatter;

    @Override
    public void initialize() {
        String formatterClass = config.getString(getConfigKey(BaseConf.SINK_FORMATTER), null);

        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }

        formatter.initialize(config, context);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields("");
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    protected abstract Logger getLogger();
}
