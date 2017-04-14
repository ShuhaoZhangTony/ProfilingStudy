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

package spark.applications.jobs;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import spark.applications.constants.FraudDetectionConstants;
import spark.applications.functions.fraud.FraudPredictor;
import spark.applications.functions.fraud.OutlierToString;
import spark.applications.functions.fraud.SplitByEntity;
import spark.applications.functions.fraud.SplitOutlier;
import spark.applications.functions.types.Outlier;
import spark.applications.functions.types.PredictionConfig;
import spark.applications.functions.types.PredictionResults;
import spark.applications.spout.MemFileSpout;
import spark.applications.util.OsUtils;

public class FraudDetectionJob extends spark.applications.jobs.AbstractJob {
    private static final Logger LOG = LogManager.getLogger(FraudDetectionJob.class);
    private int predictorThreads;
    private int batch;

    public FraudDetectionJob(String topologyName, SparkConf config) {
        super(topologyName, config);
        batch = config.getInt("batch", 1);
        count_number = config.getInt("count_number", 1);
    }

    @Override
    public void initialize(JavaStreamingContext ssc) {
       spout= MemFileSpout.load_spout(ssc, config, getConfigPrefix(), count_number, batch);
    }

    @Override
    public JavaDStream<String> buildJob(JavaStreamingContext ssc) {
        // Create the QueueInputDStream and use it do some processing
        JavaDStream<String> inputTuples = ssc.queueStream(spout);
        
        ssc.checkpoint(config.get("metric_path") + OsUtils.OS_wrapper("checkpoints"));
        
        //TODO: check if we maybe can split by entity 
        JavaPairDStream<String, String> splittedValues = inputTuples.mapToPair(new SplitByEntity());
        
        JavaPairDStream<String, PredictionResults> predResults = splittedValues.updateStateByKey(new FraudPredictor(new PredictionConfig(config)));
        JavaPairDStream<String,Outlier> singleOutlier =predResults.flatMapValues( new SplitOutlier());
        JavaDStream<String> resStream =singleOutlier.map(new OutlierToString());
        
        return resStream;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return FraudDetectionConstants.PREFIX;
    }
}
