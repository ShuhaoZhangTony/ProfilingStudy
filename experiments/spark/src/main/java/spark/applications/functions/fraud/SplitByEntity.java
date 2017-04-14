package spark.applications.functions.fraud;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SplitByEntity implements PairFunction<String, String, String> {

	private static final long serialVersionUID = -8960136352828284743L;

	@Override
	public Tuple2<String, String> call(String line) throws Exception {
		
		return new Tuple2<String, String>(line.split(",")[0],line);
	}

}
