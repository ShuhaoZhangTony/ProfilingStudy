package spark.applications.functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by szhang026 on 5/8/2016.
 */
public class SplitPair implements PairFunction<String, String, Integer> {

	private static final long serialVersionUID = -5155794207058968030L;

	@Override
    public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<String,Integer>(s, 1);
    }
}
