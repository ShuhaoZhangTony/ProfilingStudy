package flink.applications.util.data;

import com.google.common.collect.Ordering;

import java.util.Map;

public class SentimentValueOrdering extends Ordering<Map.Entry<String, Integer>> {
    @Override
    public int compare(final Map.Entry<String, Integer> status01,
                       final Map.Entry<String, Integer> status02) {
        final int sentimentValue01 = status01.getValue();
        final int sentimentValue02 = status02.getValue();
        return (sentimentValue02 - sentimentValue01);
    }
}