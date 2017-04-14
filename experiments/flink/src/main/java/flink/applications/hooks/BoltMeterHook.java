package flink.applications.hooks;

import backtype.storm.hooks.BaseTaskHook;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.TopologyContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import flink.applications.metrics.MetricsFactory;
import flink.applications.util.config.Configuration;

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
