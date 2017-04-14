package storm.applications.spout.generator;

import storm.applications.constants.BaseConstants;
import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;

import java.util.Random;

/**
 * Created by szhang026 on 8/8/2015.
 */
public class RandomNumberGenerator extends Generator {
    int send = 1;
    private Random rand;
    private long count;
    private int items = 0;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        count = config.getLong(String.format(BaseConstants.BaseConf.GENERATOR_COUNT, configPrefix), 1000000);
        rand = new Random();
    }

    @Override
    public StreamValues generate() {
        if (count-- > 0) {
            //return new StreamValues(rand.nextInt(100)+1);}
            //System.out.println("Count:"+count);
            if (send == 1) send = 2;
            else send = 1;
            //System.out.println("send"+send);
            return new StreamValues(send);
        }
        return null;
    }


}
