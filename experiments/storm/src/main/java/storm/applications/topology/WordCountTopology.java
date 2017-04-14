package storm.applications.topology;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.SplitSentenceBolt;
import storm.applications.bolt.WordCountBolt;
import storm.applications.bolt.batch.BatchSplitSentenceBolt;
import storm.applications.bolt.batch.BatchWordCountBolt;
import storm.applications.grouping.HSP_fieldsGrouping;
import storm.applications.grouping.HSP_shuffleGrouping;
import storm.applications.topology.base.BasicTopology;

import java.util.LinkedList;

import static storm.applications.constants.WordCountConstants.*;

public class WordCountTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);
    private static boolean HSP = false;

    private int splitSentenceThreads;
    private int wordCountThreads;
    private int batch = 0;
    private boolean alo;

    public WordCountTopology(String topologyName, Config config) {
        super(topologyName, config);
        splitSentenceThreads = super.config.getInt(Conf.SPLITTER_THREADS, 1);
        wordCountThreads = super.config.getInt(Conf.COUNTER_THREADS, 1);


    }

    @Override
    public LinkedList ConfigAllocation(int allocation_opt) {

        alo = config.getBoolean("alo", false);//此标识代表topology需要被调度
        if(alo){//TODO: it should read in a optimized plan, either from output of another program or being integrated in this.

            switch (allocation_opt){
                //case 0 is reserved for optimizer.
                //case 1~3 for spout test: split stay in same, near or far.
                case 1: return DiagonalPlacement1();
                case 2: return DiagonalPlacement2();
                case 3: return DiagonalPlacement3();

                //case 4~12 for split test: count stay in same, near, or far under spout same, near or far.
                case 4: return DiagonalPlacement4();
                case 5: return DiagonalPlacement5();
                case 6: return DiagonalPlacement6();
                case 7: return DiagonalPlacement7();
                case 8: return DiagonalPlacement8();
                case 9: return DiagonalPlacement9();
                case 10: return DiagonalPlacement10();
                case 11: return DiagonalPlacement11();
                case 12: return DiagonalPlacement12();

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
        //component2Node.add("socket0" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket3" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement2(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket1" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket2" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket2" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement3(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket2" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket3" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement4(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        //component2Node.add("socket0" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        //component2Node.add("socket0" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }
    private LinkedList DiagonalPlacement5(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        //component2Node.add("socket0" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket1" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }


    private LinkedList DiagonalPlacement6(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        //component2Node.add("socket0" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket2" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement7(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket1" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket1" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket2" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement8(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket1" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket2" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket2" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement9(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket1" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket3" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket2" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement10(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket2" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket2" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement11(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket2" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket3" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    private LinkedList DiagonalPlacement12(){
        // <<Which socket, which componet, how many executors>>
        LinkedList component2Node = new LinkedList<>();

        /*
        * Those ask to be allocated into socket 0 are not to be declared here, but be combined with acker thread later.
        * */

        // component2Node.add("socket0" + "," + Component.SPOUT + "," + 1);
        component2Node.add("socket2" + "," + Component.SPLITTER + "," + splitSentenceThreads);
        component2Node.add("socket0" + "," + Component.COUNTER + "," + wordCountThreads);
        component2Node.add("socket3" + "," + Component.SINK + "," + sinkThreads);
        return component2Node;
    }

    @Override
    public StormTopology buildTopology() {
        batch = config.getInt("batch");
        HSP = config.getBoolean("hsp", false);
        spout.setFields(new Fields(Field.TEXT));//output of a spouts

        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        LinkedList<Double> partition_ratio = new LinkedList<>();
        if (batch > 1) {

            if (HSP) {
                builder.setBolt(Component.SPLITTER, new BatchSplitSentenceBolt(), splitSentenceThreads)
                        .customGrouping(Component.SPOUT, new HSP_shuffleGrouping(spoutThreads, splitSentenceThreads, partition_ratio));
                builder.setBolt(Component.COUNTER, new BatchWordCountBolt(), wordCountThreads)
                        .customGrouping(Component.SPLITTER, new HSP_fieldsGrouping(new Fields(Field.WORD)));
            } else {
                builder.setBolt(Component.SPLITTER, new BatchSplitSentenceBolt(), splitSentenceThreads).shuffleGrouping(Component.SPOUT);
                builder.setBolt(Component.COUNTER, new BatchWordCountBolt(), wordCountThreads)
                        .shuffleGrouping(Component.SPLITTER);
            }

        } else {
            if (HSP) {
                for (int i = 0; i < spoutThreads*splitSentenceThreads; i++) {
                    partition_ratio.add(0.5);//simulation
                }
                builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads)
                        .customGrouping(Component.SPOUT, new HSP_shuffleGrouping(spoutThreads, splitSentenceThreads, partition_ratio));
            } else {
                builder.setBolt(Component.SPLITTER, new SplitSentenceBolt(), splitSentenceThreads).shuffleGrouping(Component.SPOUT);
                builder.setBolt(Component.COUNTER, new WordCountBolt(), wordCountThreads)
                        .fieldsGrouping(Component.SPLITTER, new Fields(Field.WORD));
            }

        }

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .shuffleGrouping(Component.COUNTER);

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
