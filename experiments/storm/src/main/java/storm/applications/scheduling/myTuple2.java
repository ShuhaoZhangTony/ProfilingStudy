package storm.applications.scheduling;

import java.io.Serializable;

/**
 * Created by szhang026 on 6/8/2016.
 */
public class myTuple2<S, I extends Number> extends scala.Tuple2 implements Serializable {
    public myTuple2(Object _1, Object _2) {
        super(_1, _2);
    }
}
