package storm.applications.constants.primitiveOperator;

import storm.applications.constants.BaseConstants;

/**
 * Created by szhang026 on 8/10/2015.
 */
public interface AggregateConstants extends BaseConstants {
    String PREFIX = "agg";

    interface Field {
        String THREAD = "thread";
        String LONG = "long";
        String WINDOW_LENGTH = "windowLength";
    }

    interface Conf extends BaseConstants.BaseConf {
        String AGGREGATE_THREADS = "agg.aggregate.threads";
        String REDUCTION_THREADS = "agg.reduction.threads";
        String COUNTER_FREQ = "agg.counter.frequency";
    }

    interface Component extends BaseConstants.BaseComponent {
        String AGGREGATE = "aggregate";
        String REDUCTION = "reduction";
    }
}
