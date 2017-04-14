package spark.applications.functions;


import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import scala.Tuple2;

/**
 * Created by I309939 on 7/21/2016.
 */
public class statefulSink implements Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> call(String word, Optional<Integer> one, State<Integer> state) {
        int sum = one.get() + (state.exists() ? state.get() : 0);

        Tuple2<String, Integer> output = new Tuple2<>(word, sum);
        state.update(sum);
        return output;
    }
}
