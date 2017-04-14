package flink.applications.topology;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.*;
import flink.applications.constants.VoIPSTREAMConstants;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.VoIPSTREAMConstants.*;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class VoIPSTREAMTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(VoIPSTREAMTopology.class);

    private int varDetectThreads;
    private int ecrThreads;
    private int rcrThreads;
    private int encrThreads;
    private int ecr24Threads;
    private int ct24Threads;
    private int fofirThreads;
    private int urlThreads;
    private int acdThreads;
    private int scorerThreads;
    private int batch;
    private int vardetectsplitThreads;

    public VoIPSTREAMTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        varDetectThreads = config.getInt(Conf.VAR_DETECT_THREADS, 1);
        vardetectsplitThreads = config.getInt(Conf.VAR_DETECT_Split_THREADS, 1);
        ecrThreads = config.getInt(Conf.ECR_THREADS, 1);
        rcrThreads = config.getInt(Conf.RCR_THREADS, 1);
        encrThreads = config.getInt(Conf.ENCR_THREADS, 1);
        ecr24Threads = config.getInt(Conf.ECR24_THREADS, 1);
        ct24Threads = config.getInt(Conf.CT24_THREADS, 1);
        fofirThreads = config.getInt(Conf.FOFIR_THREADS, 1);
        urlThreads = config.getInt(Conf.URL_THREADS, 1);
        acdThreads = config.getInt(Conf.ACD_THREADS, 1);
        scorerThreads = config.getInt(Conf.SCORER_THREADS, 1);
    }

    @Override
    public FlinkTopology buildTopology() {
        batch = config.getInt("batch");

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        if (batch > 1) {
            spout.setFields(new Fields(Field.RECORD));
            builder.setBolt(Component.VARIATION_DETECTOR_Split, new flink.applications.bolt.batch.BatchVariationDetectorSplitBolt(), vardetectsplitThreads)
                    .shuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.VARIATION_DETECTOR, new flink.applications.bolt.batch.BatchVariationDetectorBolt(), varDetectThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR_Split, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            // Filters

            builder.setBolt(Component.ECR, new flink.applications.bolt.batch.BatchECRBolt("ecr"), ecrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.RCR, new flink.applications.bolt.batch.BatchRCRBolt(), rcrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key))
                    .fieldsGrouping(Component.VARIATION_DETECTOR, Stream.BACKUP, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.ENCR, new flink.applications.bolt.batch.BatchENCRBolt(), encrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.ECR24, new flink.applications.bolt.batch.BatchECR24Bolt("ecr24"), ecr24Threads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.CT24, new flink.applications.bolt.batch.BatchCTBolt("ct24"), ct24Threads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));


            // Modules

            builder.setBolt(Component.FOFIR, new flink.applications.bolt.batch.BatchFoFiRBolt(), fofirThreads)
                    .fieldsGrouping(Component.RCR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY))
                    .fieldsGrouping(Component.ECR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY));//fixed to 1

            builder.setBolt(Component.URL, new flink.applications.bolt.batch.BatchURLBolt(), urlThreads)
                    .fieldsGrouping(Component.ENCR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY))
                    .fieldsGrouping(Component.ECR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY));//fixed to 1

            // the average must be global, so there must be a single instance doing that
            // perhaps a separate bolt, or if multiple bolts are used then a merger should
            // be employed at the end point.
            builder.setBolt(Component.GLOBAL_ACD, new flink.applications.bolt.batch.BatchGlobalACDBolt(), 1)
                    .globalGrouping(Component.VARIATION_DETECTOR);//nothing need to change

            builder.setBolt(Component.ACD, new flink.applications.bolt.batch.BatchACDBolt(), acdThreads)
                    .globalGrouping(Component.ECR24)
                    .globalGrouping(Component.CT24)
                    .globalGrouping(Component.GLOBAL_ACD);//fixed to 1


            // Score
            builder.setBolt(Component.SCORER, new flink.applications.bolt.batch.BatchScoreBolt(), scorerThreads)
                    .globalGrouping(Component.FOFIR)
                    .globalGrouping(Component.URL)
                    .globalGrouping(Component.ACD);//fixed to 1
        } else {
            spout.setFields(new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME, Field.RECORD));
            builder.setBolt(Component.VARIATION_DETECTOR, new VariationDetectorBolt(), varDetectThreads)
                    .fieldsGrouping(Component.SPOUT, new Fields(Field.CALLING_NUM, Field.CALLED_NUM));

            // Filters

            builder.setBolt(Component.ECR, new ECRBolt("ecr"), ecrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));

            builder.setBolt(Component.RCR, new RCRBolt(), rcrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM))
                    .fieldsGrouping(Component.VARIATION_DETECTOR, Stream.BACKUP, new Fields(Field.CALLED_NUM));

            builder.setBolt(Component.ENCR, new ENCRBolt(), encrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));

            builder.setBolt(Component.ECR24, new ECRBolt("ecr24"), ecr24Threads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));

            builder.setBolt(Component.CT24, new CTBolt("ct24"), ct24Threads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(Field.CALLING_NUM));

            // Modules

            builder.setBolt(Component.FOFIR, new FoFiRBolt(), fofirThreads)
                    .fieldsGrouping(Component.RCR, new Fields(Field.CALLING_NUM))
                    .fieldsGrouping(Component.ECR, new Fields(Field.CALLING_NUM));


            builder.setBolt(Component.URL, new URLBolt(), urlThreads)
                    .fieldsGrouping(Component.ENCR, new Fields(Field.CALLING_NUM))
                    .fieldsGrouping(Component.ECR, new Fields(Field.CALLING_NUM));

            // the average must be global, so there must be a single instance doing that
            // perhaps a separate bolt, or if multiple bolts are used then a merger should
            // be employed at the end point.
            builder.setBolt(Component.GLOBAL_ACD, new GlobalACDBolt(), 1)
                    .allGrouping(Component.VARIATION_DETECTOR);

            builder.setBolt(Component.ACD, new ACDBolt(), acdThreads)
                    .fieldsGrouping(Component.ECR24, new Fields(Field.CALLING_NUM))
                    .fieldsGrouping(Component.CT24, new Fields(Field.CALLING_NUM))
                    .allGrouping(Component.GLOBAL_ACD);


            // Score
            builder.setBolt(Component.SCORER, new ScoreBolt(), scorerThreads)
                    .fieldsGrouping(Component.FOFIR, new Fields(Field.CALLING_NUM))
                    .fieldsGrouping(Component.URL, new Fields(Field.CALLING_NUM))
                    .fieldsGrouping(Component.ACD, new Fields(Field.CALLING_NUM));
        }
        builder.setBolt(Component.SINK, sink, sinkThreads)
                .fieldsGrouping(Component.SCORER, new Fields(Field.CALLING_NUM));

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
