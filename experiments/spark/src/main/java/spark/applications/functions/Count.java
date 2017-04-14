package spark.applications.functions;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.Function2;

import java.util.List;

/**
 * Created by I309939 on 5/3/2016.
 */


/**
 * A two-argument function that takes arguments of type T1 and T2 and returns an R.
 */
public class Count implements Function2<List<Integer>, Optional<Integer>, Optional<Integer>> {
    @Override
    public Optional<Integer> call(List<Integer> value, Optional<Integer> sum) throws Exception {
        Integer newSum = sum.or(0);
          for (Integer i : value) {
            newSum += i;
        }
         return Optional.of(newSum);
    }
}
