package flink.applications.constants.primitiveOperator;

import flink.applications.constants.BaseConstants;

/**
 * Created by szhang026 on 8/8/2015.
 */
public interface FilterConstants extends BaseConstants {
    String PREFIX = "fc";

    interface Field {
        String INT = "int";

    }

    interface Conf extends BaseConf {
        String Filter_THREADS = "fc.filter.threads";
    }

    interface Component extends BaseComponent {
        String FILTER = "filter";
    }
}
