package storm.applications.bolt.batch;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.util.geoip.IPLocation;
import storm.applications.util.geoip.IPLocationFactory;
import storm.applications.util.geoip.Location;

import static storm.applications.constants.ClickAnalyticsConstants.BaseConf;
import static storm.applications.constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class BatchGeographyBolt extends AbstractBolt {
    private IPLocation resolver;

    @Override
    public void initialize() {
        String ipResolver = config.getString(BaseConf.GEOIP_INSTANCE);
        resolver = IPLocationFactory.create(ipResolver, config);
    }

    @Override
    public void execute(Tuple input) {

        String[] OIP = ((String[]) input.getValue(0));
        int batch = OIP.length;
        for (int i = 0; i < batch; i++) {
            String ip = OIP[i];
            Location location = resolver.resolve(ip);
            if (location != null) {
                String city = location.getCity();
                String country = location.getCountryName();

                collector.emit(input, new Values(country, city));
            }
        }
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.CITY);
    }
}
