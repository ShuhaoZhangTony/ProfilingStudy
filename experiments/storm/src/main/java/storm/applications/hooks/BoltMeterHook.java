package storm.applications.hooks;

import org.apache.storm.hooks.BaseTaskHook;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.task.TopologyContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import storm.applications.metrics.MetricsFactory;
import storm.applications.util.config.Configuration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author mayconbordin
 */
public class BoltMeterHook extends BaseTaskHook {
    public Meter emittedTuples;
    public Meter receivedTuples;
    public Timer executeLatency;
    private Configuration config;

    @Override
    public void prepare(Map conf, TopologyContext context) {
        config = Configuration.fromMap(conf);

        MetricRegistry registry = MetricsFactory.createRegistry(config);

        String componentId = context.getThisComponentId();
        String taskId = String.valueOf(context.getThisTaskId());

        emittedTuples = registry.meter(MetricRegistry.name("emitted", componentId, taskId));
        receivedTuples = registry.meter(MetricRegistry.name("received", componentId, taskId));
        executeLatency = registry.timer(MetricRegistry.name("execute-latency", componentId, taskId));
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
        receivedTuples.mark();

        if (info.executeLatencyMs != null) {
            executeLatency.update(info.executeLatencyMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void emit(EmitInfo info) {
        emittedTuples.mark();
    }
}
