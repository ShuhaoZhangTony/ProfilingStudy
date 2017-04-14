package storm.applications.bolt.primitiveOperator;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.primitiveOperator.FilterConstants.Field;

/**
 * Created by szhang026 on 8/7/2015.
 */
public class FilterBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt.class);

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.INT);
    }

    @Override
    public void execute(Tuple input) {//all input are integers

        //String[] words = input.getString(0).split(splitregex);

//        for (int word : words) {
//            if (!StringUtils.isBlank(word))
//                collector.emit(input, new Values(word));
//        }
//
//        collector.ack(input);
        //long threadId = Thread.currentThread().getId();
        //LOG.warn("1:Thread # " + threadId + " is doing this task");
        int element = (int) input.getInteger(0);
        if (element <= 5) {//range Filter
            collector.emit(input, new Values(element));
            System.out.println(element);
        }
        collector.ack(input);
    }
}
