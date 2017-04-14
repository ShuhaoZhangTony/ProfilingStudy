package storm.applications.bolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.WordCountConstants.Field;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt_compute extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt_compute.class);
    private final Map<String, MutableLong> counts = new HashMap<>();
    private final double[][] a = random(25, 25);
    private final double[][] b = random(25, 25);
    //AtomicInteger index_e= new AtomicInteger();
    // long start=0,end=0,curr=0;
    // private static LinkedList<String> logger=new LinkedList<String>();
    // transient private BufferedWriter writer ;
    // int curr=0;
    private int total_thread = 0;

    public static double[][] random(int m, int n) {
        double[][] C = new double[m][n];
        for (int i = 0; i < m; i++)
            for (int j = 0; j < n; j++)
                C[i][j] = Math.random();
        return C;
    }

    // return C = A * B
    public static void multiply(double[][] A, double[][] B) {
        int mA = A.length;
        int nA = A[0].length;
        int mB = B.length;
        int nB = B[0].length;
        double tempv;
        if (nA != mB) throw new RuntimeException("Illegal matrix dimensions.");
        //double[][] C = new double[mA][nB];
        for (int i = 0; i < mA; i++)
            for (int j = 0; j < nB; j++)
                for (int k = 0; k < nA; k++)
                    tempv = A[i][k] * B[k][j];
        //return C;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField(Field.WORD);
//        MutableLong count = counts.get(word);
//        
//        if (count == null) {
//            count = new MutableLong(0);
//            counts.put(word, count);
//        }
//        count.increment();
        //multiply(a,b);
        int temp = 0;
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 1000; j++) {
                temp += i * j;//4 cycles.
            }
        }

        collector.emit(input, new Values(word, temp));
        collector.ack(input);

    }

}
