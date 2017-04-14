package storm.applications.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.*;
import storm.applications.sink.BaseSink;
import storm.applications.spout.AbstractSpout;
import storm.applications.topology.base.AbstractTopology;

import static storm.applications.constants.LogProcessingConstants.*;

/**
 * https://github.com/ashrithr/LogEventsProcessing
 *
 * @author Ashrith Mekala <ashrith@me.com>
 */
public class LogProcessingTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LogProcessingTopology.class);

    private AbstractSpout spout;
    private BaseSink countSink;
    private BaseSink statusSink;
    private BaseSink countrySink;

    private int spoutThreads;
    private int countSinkThreads;
    private int statusSinkThreads;
    private int countrySinkThreads;
    private int volumeCountThreads;
    private int statusCountThreads;
    private int geoFinderThreads;
    private int geoStatsThreads;
    private int batch;

    public LogProcessingTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        spout = loadSpout();
        countSink = loadSink("count");
        statusSink = loadSink("status");
        countrySink = loadSink("country");

        spoutThreads = config.getInt(getConfigKey(Conf.SPOUT_THREADS), 1);
        countSinkThreads = config.getInt(getConfigKey(Conf.SINK_THREADS, "count"), 1);
        statusSinkThreads = config.getInt(getConfigKey(Conf.SINK_THREADS, "status"), 1);
        countrySinkThreads = config.getInt(getConfigKey(Conf.SINK_THREADS, "country"), 1);

        volumeCountThreads = config.getInt(Conf.VOLUME_COUNTER_THREADS, 1);
        statusCountThreads = config.getInt(Conf.STATUS_COUNTER_THREADS, 1);
        geoFinderThreads = config.getInt(Conf.GEO_FINDER_THREADS, 1);
        geoStatsThreads = config.getInt(Conf.GEO_STATS_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        batch = config.getInt("batch");

        spout.setFields(new Fields(Field.IP, Field.TIMESTAMP, Field.TIMESTAMP_MINUTES,
                Field.REQUEST, Field.RESPONSE, Field.BYTE_SIZE));

        builder.setSpout(Component.SPOUT, spout, spoutThreads);


        if (batch > 1) {
            builder.setBolt(Component.VOLUME_COUNTER, new storm.applications.bolt.batch.BatchVolumeCountBolt(), volumeCountThreads)
                    .shuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.STATUS_COUNTER, new storm.applications.bolt.batch.BatchStatusCountBolt(), statusCountThreads)
                    .shuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.GEO_FINDER, new storm.applications.bolt.batch.BatchGeographyBolt(), geoFinderThreads)
                    .shuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.GEO_STATS, new storm.applications.bolt.batch.BatchGeoStatsBolt(), geoStatsThreads)
                    .shuffleGrouping(Component.GEO_FINDER);
        } else {
            builder.setBolt(Component.VOLUME_COUNTER, new VolumeCountBolt(), volumeCountThreads)
                    .fieldsGrouping(Component.SPOUT, new Fields(Field.TIMESTAMP_MINUTES));

            builder.setBolt(Component.STATUS_COUNTER, new StatusCountBolt(), statusCountThreads)
                    .fieldsGrouping(Component.SPOUT, new Fields(Field.RESPONSE));

            builder.setBolt(Component.GEO_FINDER, new GeographyBolt(), geoFinderThreads)
                    .shuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.GEO_STATS, new GeoStatsBolt(), geoStatsThreads)
                    .fieldsGrouping(Component.GEO_FINDER, new Fields(Field.COUNTRY));
            //.shuffleGrouping(Component.GEO_FINDER);
        }

        builder.setBolt(Component.VOLUME_SINK, countSink, countSinkThreads)
                .shuffleGrouping(Component.VOLUME_COUNTER);

        builder.setBolt(Component.STATUS_SINK, statusSink, statusSinkThreads)
                .shuffleGrouping(Component.STATUS_COUNTER);

        builder.setBolt(Component.GEO_SINK, countrySink, countrySinkThreads)
                .shuffleGrouping(Component.GEO_STATS);

        return builder.createTopology();
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
