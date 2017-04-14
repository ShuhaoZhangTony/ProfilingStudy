package spark.applications.functions.original;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * Created by I309939 on 5/3/2016.
 */
public class Count implements Function2<List<Long>, Optional<Long>, Optional<Long>> {
//    private final Map<String, MutableLong> counts = new HashMap<>();
//
//    @Override
//    public Tuple2<String, Long> call(String inputTuple) throws Exception {
//
//
//        MutableLong count = counts.get(inputTuple);
//        if (count == null) {
//            count = new MutableLong(0);
//            counts.put(inputTuple, count);
//        }
//        count.increment();
//        return new Tuple2(inputTuple, count.getValue());
//    }

    @Override
    public Optional<Long> call(List<Long> values, Optional<Long> state) throws Exception {
        if (values == null || values.isEmpty()) {
            return state;
        }
        long sum = 0L;
        for (Long v : values) {
            sum += v;
        }
        return Optional.of(state.or(0L) + sum);
    }
}
