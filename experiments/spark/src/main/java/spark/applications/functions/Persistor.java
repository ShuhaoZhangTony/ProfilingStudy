package spark.applications.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by szhang026 on 5/4/2016.
 */
public class Persistor implements Function<JavaRDD<Iterable<Tuple2<String, Long>>>, JavaRDD<Iterable<Tuple2<String, Long>>>> {


    private transient PrintWriter outLogger = null;
    private long currentTime = 0;

    {
        try {
            outLogger = new PrintWriter(new BufferedWriter(new FileWriter("metric_output\\spark-output-rate.csv", true)));
            outLogger.println("-------- New Session ------<date-time>, <wall-clock-time(s)>, <physical-tuples-in-last-period>");
            outLogger.println("Date, Wall clock time (s), TTuples");
            outLogger.flush();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }


    @Override
    public JavaRDD<Iterable<Tuple2<String, Long>>> call(JavaRDD<Iterable<Tuple2<String, Long>>> arg0) throws Exception {
        currentTime = System.currentTimeMillis();
        Date date = new Date(currentTime);
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

        outLogger.println(formatter.format(date) + "," + currentTime + "," + arg0.count());
        outLogger.flush();

        return arg0;
    }
}
