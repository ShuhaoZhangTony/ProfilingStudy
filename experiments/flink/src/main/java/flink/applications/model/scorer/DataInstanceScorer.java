package flink.applications.model.scorer;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import java.util.List;

/**
 * DataInstanceScorer defines the method to calculate the data instance anomaly scores.
 *
 * @param <T>
 * @author yexijiang
 */
public abstract class DataInstanceScorer<T> {
    /**
     * Emit the calculated score to downstream.
     *
     * @param collector
     * @param observationList
     */
    public void calculateScores(OutputCollector collector, List<T> observationList) {
        List<ScorePackage> packageList = getScores(observationList);
        for (ScorePackage scorePackage : packageList) {
            collector.emit(new Values(scorePackage.getId(), scorePackage.getScore(), scorePackage.getObj()));
        }
    }

    /**
     * Calculate the data instance anomaly score for given data instances and directly send to downstream.
     *
     * @param observationList
     * @return
     */
    public abstract List<ScorePackage> getScores(List<T> observationList);
}
