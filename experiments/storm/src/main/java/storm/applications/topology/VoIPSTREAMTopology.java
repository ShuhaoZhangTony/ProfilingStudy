package storm.applications.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.*;
import storm.applications.constants.VoIPSTREAMConstants;
import storm.applications.topology.base.BasicTopology;

import java.util.LinkedList;

import static storm.applications.constants.VoIPSTREAMConstants.*;

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

        varDetectThreads =  super.config.getInt(Conf.VAR_DETECT_THREADS, 1);
        vardetectsplitThreads =  super.config.getInt(Conf.VAR_DETECT_Split_THREADS, 1);
        ecrThreads =  super.config.getInt(Conf.ECR_THREADS, 1);
        rcrThreads =  super.config.getInt(Conf.RCR_THREADS, 1);
        encrThreads =  super.config.getInt(Conf.ENCR_THREADS, 1);
        ecr24Threads =  super.config.getInt(Conf.ECR24_THREADS, 1);
        ct24Threads =  super.config.getInt(Conf.CT24_THREADS, 1);
        fofirThreads =  super.config.getInt(Conf.FOFIR_THREADS, 1);
        urlThreads =  super.config.getInt(Conf.URL_THREADS, 1);
        acdThreads =  super.config.getInt(Conf.ACD_THREADS, 1);
        scorerThreads =  super.config.getInt(Conf.SCORER_THREADS, 1);


    }

    @Override
    public StormTopology buildTopology() {
        batch = config.getInt("batch");

        builder.setSpout(Component.SPOUT, spout, spoutThreads);

        if (batch > 1) {
            spout.setFields(new Fields(Field.RECORD));
            builder.setBolt(Component.VARIATION_DETECTOR_Split, new storm.applications.bolt.batch.BatchVariationDetectorSplitBolt(), vardetectsplitThreads)
                    .shuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.VARIATION_DETECTOR, new storm.applications.bolt.batch.BatchVariationDetectorBolt(), varDetectThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR_Split, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            // Filters

            builder.setBolt(Component.ECR, new storm.applications.bolt.batch.BatchECRBolt("ecr"), ecrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.RCR, new storm.applications.bolt.batch.BatchRCRBolt(), rcrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key))
                    .fieldsGrouping(Component.VARIATION_DETECTOR, Stream.BACKUP, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.ENCR, new storm.applications.bolt.batch.BatchENCRBolt(), encrThreads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.ECR24, new storm.applications.bolt.batch.BatchECR24Bolt("ecr24"), ecr24Threads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));

            builder.setBolt(Component.CT24, new storm.applications.bolt.batch.BatchCTBolt("ct24"), ct24Threads)
                    .fieldsGrouping(Component.VARIATION_DETECTOR, new Fields(VoIPSTREAMConstants.Field.VARIATION_DETECTOR_Split_Key));


            // Modules

            builder.setBolt(Component.FOFIR, new storm.applications.bolt.batch.BatchFoFiRBolt(), fofirThreads)
                    .fieldsGrouping(Component.RCR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY))
                    .fieldsGrouping(Component.ECR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY));//fixed to 1

            builder.setBolt(Component.URL, new storm.applications.bolt.batch.BatchURLBolt(), urlThreads)
                    .fieldsGrouping(Component.ENCR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY))
                    .fieldsGrouping(Component.ECR, new Fields(VoIPSTREAMConstants.Field.RATE_KEY));//fixed to 1

            // the average must be global, so there must be a single instance doing that
            // perhaps a separate bolt, or if multiple bolts are used then a merger should
            // be employed at the end point.
            builder.setBolt(Component.GLOBAL_ACD, new storm.applications.bolt.batch.BatchGlobalACDBolt(), 1)
                    .globalGrouping(Component.VARIATION_DETECTOR);//nothing need to change

            builder.setBolt(Component.ACD, new storm.applications.bolt.batch.BatchACDBolt(), acdThreads)
                    .globalGrouping(Component.ECR24)
                    .globalGrouping(Component.CT24)
                    .globalGrouping(Component.GLOBAL_ACD);//fixed to 1


            // Score
            builder.setBolt(Component.SCORER, new storm.applications.bolt.batch.BatchScoreBolt(), scorerThreads)
                    .globalGrouping(Component.FOFIR)
                    .globalGrouping(Component.URL)
                    .globalGrouping(Component.ACD);//fixed to 1
        } else {
            spout.setFields(new Fields(Field.CALLING_NUM, Field.CALLED_NUM, Field.ANSWER_TIME,Field.RECORD));
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

        return builder.createTopology();
    }

    public LinkedList ConfigAllocation(int allocation_opt){
        LOG.info("VS configure allocation ing...");
        boolean alo = config.getBoolean("alo", false);//此标识代表topology需要被调度
        if(alo){//TODO: it should read in a optimized plan, either from output of another program or being integrated in this.

         switch (allocation_opt){
             //case 0 is reserved for optimizer.
             //case 1~3 are used for preliminary test
             case 1: return DiagonalPlacement1();
             case 2: return DiagonalPlacement2();
             case 3: return DiagonalPlacement3();

             //case 4~6 are used to test the effect of different co-location of VD.
             case 4:return DiagonalPlacement4();
             case 5:return DiagonalPlacement5();
             case 6:return DiagonalPlacement6();
         }
        }
        return null;
    }
    private LinkedList DiagonalPlacement1(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);

        component2Node.add("socket2" + "," + Component.VARIATION_DETECTOR + "," + varDetectThreads);

        //  component2Node.add("socket0" + "," + Component.RCR + "," + rcrThreads);

        //  component2Node.add("socket0" + "," + Component.ECR + "," + ecrThreads);

        //component2Node.add("socket0" + "," + Component.ENCR + "," + encrThreads);

        //component2Node.add("socket0" + "," + Component.CT24 + "," + ct24Threads);

        //   component2Node.add("socket0" + "," + Component.ECR24 + "," + ecr24Threads);

        //   component2Node.add("socket0" + "," + Component.GLOBAL_ACD + "," + 1);

        component2Node.add("socket1" + "," + Component.FOFIR + "," + fofirThreads);

        component2Node.add("socket3" + "," + Component.URL + "," + urlThreads);

        component2Node.add("socket3" + "," + Component.ACD + "," + acdThreads);

        component2Node.add("socket1" + "," + Component.SCORER + "," + scorerThreads);

        //    component2Node.add("socket0" + "," + Component.SINK + "," + 1);

        return component2Node;
    }
    private LinkedList DiagonalPlacement2(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

       // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);

      //  component2Node.add("socket0" + "," + Component.VARIATION_DETECTOR + "," + varDetectThreads);

      //  component2Node.add("socket0" + "," + Component.RCR + "," + rcrThreads);

      //  component2Node.add("socket0" + "," + Component.ECR + "," + ecrThreads);

        component2Node.add("socket1" + "," + Component.ENCR + "," + encrThreads);

        component2Node.add("socket1" + "," + Component.CT24 + "," + ct24Threads);

        component2Node.add("socket1" + "," + Component.ECR24 + "," + ecr24Threads);

        component2Node.add("socket1" + "," + Component.GLOBAL_ACD + "," + 1);

        component2Node.add("socket2" + "," + Component.FOFIR + "," + fofirThreads);

        component2Node.add("socket2" + "," + Component.URL + "," + urlThreads);

        component2Node.add("socket3" + "," + Component.ACD + "," + acdThreads);

        component2Node.add("socket3" + "," + Component.SCORER + "," + scorerThreads);

        component2Node.add("socket3" + "," + Component.SINK + "," + 1);

        return component2Node;
    }
    private LinkedList DiagonalPlacement3(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);

          component2Node.add("socket1" + "," + Component.VARIATION_DETECTOR + "," + varDetectThreads/2);

        //  component2Node.add("socket0" + "," + Component.RCR + "," + rcrThreads);

        //  component2Node.add("socket0" + "," + Component.ECR + "," + ecrThreads);

        component2Node.add("socket1" + "," + Component.ENCR + "," + encrThreads);

        component2Node.add("socket1" + "," + Component.CT24 + "," + ct24Threads);

        component2Node.add("socket1" + "," + Component.ECR24 + "," + ecr24Threads);

        component2Node.add("socket1" + "," + Component.GLOBAL_ACD + "," + 1);

        component2Node.add("socket2" + "," + Component.FOFIR + "," + fofirThreads/2);

        component2Node.add("socket3" + "," + Component.FOFIR + "," + fofirThreads/2);

        component2Node.add("socket2" + "," + Component.URL + "," + urlThreads);

        component2Node.add("socket3" + "," + Component.ACD + "," + acdThreads);

        component2Node.add("socket3" + "," + Component.SCORER + "," + scorerThreads);

        component2Node.add("socket3" + "," + Component.SINK + "," + 1);

        return component2Node;
    }

    /*Spout VD same placement (0,0), test VD
    * */
    private LinkedList DiagonalPlacement4(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);

        //component2Node.add("socket0" + "," + Component.VARIATION_DETECTOR + "," + 1);

        component2Node.add("socket1" + "," + Component.RCR + "," + rcrThreads);

        component2Node.add("socket1" + "," + Component.ECR + "," + ecrThreads);

        component2Node.add("socket1" + "," + Component.ENCR + "," + encrThreads);

        component2Node.add("socket1" + "," + Component.CT24 + "," + ct24Threads);

        component2Node.add("socket1" + "," + Component.ECR24 + "," + ecr24Threads);

        component2Node.add("socket1" + "," + Component.GLOBAL_ACD + "," + 1);

        component2Node.add("socket2" + "," + Component.FOFIR + "," + fofirThreads);

        component2Node.add("socket2" + "," + Component.URL + "," + urlThreads);

        component2Node.add("socket2" + "," + Component.ACD + "," + acdThreads);

        component2Node.add("socket3" + "," + Component.SCORER + "," + scorerThreads);

        component2Node.add("socket3" + "," + Component.SINK + "," + 1);

        return component2Node;
    }

    /*Spout VD different placement (0,1), test VD, VD occupy socket 1 alone.
* */
    private LinkedList DiagonalPlacement5(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);

        component2Node.add("socket1" + "," + Component.VARIATION_DETECTOR + "," + 1);

        component2Node.add("socket3" + "," + Component.RCR + "," + rcrThreads);

        component2Node.add("socket3" + "," + Component.ECR + "," + ecrThreads);

        component2Node.add("socket3" + "," + Component.ENCR + "," + encrThreads);

        component2Node.add("socket3" + "," + Component.CT24 + "," + ct24Threads);

        component2Node.add("socket3" + "," + Component.ECR24 + "," + ecr24Threads);

        component2Node.add("socket3" + "," + Component.GLOBAL_ACD + "," + 1);

        component2Node.add("socket2" + "," + Component.FOFIR + "," + fofirThreads);

        component2Node.add("socket2" + "," + Component.URL + "," + urlThreads);

        component2Node.add("socket2" + "," + Component.ACD + "," + acdThreads);

        component2Node.add("socket3" + "," + Component.SCORER + "," + scorerThreads);

        component2Node.add("socket3" + "," + Component.SINK + "," + 1);

        return component2Node;
    }

    /*Spout VD different placement (0,2), test VD, VD occupy socket 2 alone.
 * */
    private LinkedList DiagonalPlacement6(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);

        component2Node.add("socket2" + "," + Component.VARIATION_DETECTOR + "," + 1);

        component2Node.add("socket3" + "," + Component.RCR + "," + rcrThreads);

        component2Node.add("socket3" + "," + Component.ECR + "," + ecrThreads);

        component2Node.add("socket3" + "," + Component.ENCR + "," + encrThreads);

        component2Node.add("socket3" + "," + Component.CT24 + "," + ct24Threads);

        component2Node.add("socket3" + "," + Component.ECR24 + "," + ecr24Threads);

        component2Node.add("socket3" + "," + Component.GLOBAL_ACD + "," + 1);

        component2Node.add("socket1" + "," + Component.FOFIR + "," + fofirThreads);

        component2Node.add("socket1" + "," + Component.URL + "," + urlThreads);

        component2Node.add("socket1" + "," + Component.ACD + "," + acdThreads);

        component2Node.add("socket3" + "," + Component.SCORER + "," + scorerThreads);

        component2Node.add("socket3" + "," + Component.SINK + "," + 1);

        return component2Node;
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
