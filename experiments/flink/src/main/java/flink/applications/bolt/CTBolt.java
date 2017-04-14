package flink.applications.bolt;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractFilterBolt;
import flink.applications.model.cdr.CallDetailRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static flink.applications.constants.VoIPSTREAMConstants.Field;

/**
 * Per-user total call time
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CTBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CTBolt.class);

    public CTBolt(String configPrefix) {
        super(configPrefix, Field.CALLTIME, null);
    }

    @Override
    public void execute(Tuple input) {
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        boolean newCallee = input.getBooleanByField(Field.NEW_CALLEE);

        if (cdr.isCallEstablished() && newCallee) {
            String caller = input.getStringByField(Field.CALLING_NUM);
            long timestamp = cdr.getAnswerTime().getMillis() / 1000;

            filter.add(caller, cdr.getCallDuration(), timestamp);
            double calltime = filter.estimateCount(caller, timestamp);

            LOG.debug(String.format("CallTime: %f", calltime));
            collector.emit(new Values(caller, timestamp, calltime, cdr));
        }
        collector.ack(input);
    }
}