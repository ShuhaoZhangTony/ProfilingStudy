package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.SplitSentenceBolt;
import flink.applications.bolt.WordCountBolt;
import flink.applications.bolt.batch.BatchSplitSentenceBolt;
import flink.applications.bolt.batch.BatchWordCountBolt;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.WordCountConstants.*;

public class WordCountTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    private int splitSentenceThreads;
    private int wordCountThreads;
    private int batch = 0;

    public WordCountTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        splitSentenceThreads = config.getInt(Conf.SPLITTER_THREADS, 1);
        wordCountThreads = config.getInt(Conf.COUNTER_THREADS, 1);
    }

    @Override
    public FlinkTopology buildTopology() {
        batch = config.getInt("batch");
        spout.setFields(new Fields(Field.TEXT));//output of a spouts

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        if (batch > 1)
            builder.setBolt(Component.SPLITTER, new BatchSplitSentenceBolt(), splitSentenceThreads).shuffleGrouping(Component.SPOUT);
        else
            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads).shuffleGrouping(Component.SPOUT);

        if (batch > 1)
            builder.setBolt(Component.COUNTER, new BatchWordCountBolt(), wordCountThreads)
                    .shuffleGrouping(Component.SPLITTER);
        else
            builder.setBolt(Component.COUNTER, new WordCountBolt(), wordCountThreads)
                    .fieldsGrouping(Component.SPLITTER, new Fields(Field.WORD));

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.COUNTER);
        return FlinkTopology.createTopology(builder);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }

}
