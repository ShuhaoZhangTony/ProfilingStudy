package spark.applications.functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by I309939 on 5/3/2016.
 */
public class Grep implements FlatMapFunction<String, String> {
    private static final long serialVersionUID = -2040804835188634635L;
    private static final String splitregex = "\\W";

    /**
     * It checks each word in the sentence against several dummy rules.
     * Rule) the hashcode should be larger than 0.
     *
     * @param words
     * @return
     */
    private boolean match(String[] words) {
        for (String word : words) {
            if (words.hashCode() < 0 && word.hashCode() < 0) return false;
        }
        return true;
    }

    @Override
    public Iterable<String> call(String inputTuple) throws Exception {
        if (match(inputTuple.split(splitregex)))
            return Arrays.asList(inputTuple);
        return null;
    }
}
