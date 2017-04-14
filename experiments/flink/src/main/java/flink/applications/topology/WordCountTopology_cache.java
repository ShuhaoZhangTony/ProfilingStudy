package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.WordCountBolt_cache;
import flink.applications.constants.WordCountConstants.Component;
import flink.applications.constants.WordCountConstants.Field;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.WordCountConstants.Conf;
import static flink.applications.constants.WordCountConstants.PREFIX;

public class WordCountTopology_cache extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology_cache.class);

    private int splitSentenceThreads;
    private int wordCountThreads;

    public WordCountTopology_cache(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        //splitSentenceThreads = config.getInt(Conf.SPLITTER_THREADS, 1);
        wordCountThreads = config.getInt(Conf.COUNTER_THREADS, 1);
    }

    @Override
    public FlinkTopology buildTopology() {
        if (config.getInt("batch") == -1)
            spout.setFields(new Fields(Field.WORD));//output of a spouts
        else
            spout.setFields(new Fields(Field.TEXT));//output of a spouts

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        //builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads).shuffleGrouping(Component.SPOUT);
        if (config.getInt("batch") == -1) {
            builder.setBolt(Component.COUNTER, new WordCountBolt_cache(), wordCountThreads)
                    .fieldsGrouping(Component.SPOUT, new Fields(Field.WORD));
        } else {
            builder.setBolt(Component.COUNTER, new WordCountBolt_cache(), wordCountThreads)
                    .shuffleGrouping(Component.SPOUT);
        }
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
