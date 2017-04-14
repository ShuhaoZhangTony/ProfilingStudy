package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.WordCountBolt_compute;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.WordCountConstants.*;

public class WordCountTopology_compute extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology_compute.class);

    private int splitSentenceThreads;
    private int wordCountThreads;

    public WordCountTopology_compute(String topologyName, Config config) {
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
        spout.setFields(new Fields(Field.WORD));//output of a spouts

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        //builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads).shuffleGrouping(Component.SPOUT);

        builder.setBolt(Component.COUNTER, new WordCountBolt_compute(), wordCountThreads)
                .fieldsGrouping(Component.SPOUT, new Fields(Field.WORD));

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
