package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.model.scorer.DataInstanceScorer;
import flink.applications.model.scorer.DataInstanceScorerFactory;
import flink.applications.model.scorer.ScorePackage;

import java.util.ArrayList;
import java.util.List;

import static flink.applications.constants.MachineOutlierConstants.Conf;
import static flink.applications.constants.MachineOutlierConstants.Field;

public class ObservationScoreBolt extends AbstractBolt {
    private long previousTimestamp;
    private String dataTypeName;
    private DataInstanceScorer dataInstanceScorer;
    private List<Object> observationList;

    @Override
    public void initialize() {
        previousTimestamp = 0;
        dataTypeName = config.getString(Conf.SCORER_DATA_TYPE);
        observationList = new ArrayList<>();
        dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(Field.TIMESTAMP);

        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit 
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                for (ScorePackage scorePackage : scorePackageList) {
                    collector.emit(new Values(scorePackage.getId(), scorePackage.getScore(),
                            previousTimestamp, scorePackage.getObj()));
                }
                observationList.clear();
            }

            previousTimestamp = timestamp;
        }

        observationList.add(input.getValueByField(Field.OBSERVATION));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.DATAINST_ANOMALY_SCORE, Field.TIMESTAMP, Field.OBSERVATION);
    }
}
