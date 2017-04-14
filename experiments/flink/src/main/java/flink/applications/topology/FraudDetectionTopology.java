/*
 * beymani: Outlier and anamoly detection 
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package flink.applications.topology;


import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import flink.applications.bolt.FraudPredictorBolt;
import flink.applications.bolt.batch.BatchFraudPredictorBolt;
import flink.applications.topology.base.BasicTopology;
import org.apache.flink.storm.api.FlinkTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.FraudDetectionConstants.*;

/**
 * Storm topolgy driver for outlier detection
 *
 * @author pranab
 */
public class FraudDetectionTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetectionTopology.class);

    private int predictorThreads;

    private int batch;
    private Number predictorSplitThreads;

    public FraudDetectionTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();

        predictorThreads = config.getInt(Conf.PREDICTOR_THREADS, 1);
        predictorSplitThreads = config.getInt(Conf.PREDICTORSPLIT_THREADS, 1);
    }

    @Override
    public FlinkTopology buildTopology() {
        batch = config.getInt("batch");


        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        //.setNumTasks(spoutThreads);
        if (batch > 1) {
            spout.setFields(new Fields(Field.RECORD_DATA, Field.RECORD_KEY));
//            builder.setBolt(Component.PREDICTOR_Split, new BatchFraudPredictor_SplitBolt(), predictorSplitThreads)
//                    .localOrShuffleGrouping(Component.SPOUT);

            builder.setBolt(Component.PREDICTOR, new BatchFraudPredictorBolt(), predictorThreads)
                    .fieldsGrouping(Component.SPOUT, new Fields(Field.RECORD_KEY));
        } else {
            spout.setFields(new Fields(Field.ENTITY_ID, Field.RECORD_DATA));
            builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt(), predictorThreads)
                    .fieldsGrouping(Component.SPOUT, new Fields(Field.ENTITY_ID));
        }

        builder.setBolt(Component.SINK, sink, sinkThreads)
                .fieldsGrouping(Component.PREDICTOR, new Fields(Field.ENTITY_ID));
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
