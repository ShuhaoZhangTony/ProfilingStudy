package spark.applications.functions.fraud;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import spark.applications.functions.types.Outlier;

public class OutlierToString implements Function<Tuple2<String,Outlier>,String> {

	private static final long serialVersionUID = -981642143905019942L;

	@Override
	public String call(Tuple2<String, Outlier> v1) throws Exception {
		
		return v1._2.toString();
	}

}
