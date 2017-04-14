package flink.applications.spout.generator;

import flink.applications.constants.VoIPSTREAMConstants.Conf;
import flink.applications.model.cdr.CDRDataGenerator;
import flink.applications.model.cdr.CallDetailRecord;
import flink.applications.util.config.Configuration;
import flink.applications.util.math.RandomUtil;
import flink.applications.util.stream.StreamValues;
import org.joda.time.DateTime;

import java.util.Random;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CDRGenerator extends Generator {
    private String[] phoneNumbers;
    private int population;
    private double errorProb;
    private Random rand = new Random();

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);

        population = config.getInt(Conf.GENERATOR_POPULATION, 50);
        errorProb = config.getDouble(Conf.GENERATOR_POPULATION, 0.05);

        phoneNumbers = new String[population];
        System.out.println("population:" + population);
        try {
            for (int i = 0; i < population; i++) {
                phoneNumbers[i] = CDRDataGenerator.phoneNumber("US", 11);
            }
        } catch (Exception ex) {
            System.out.println("Fuck, whatever error, throw it!" + ex.getMessage());
            System.exit(-1);
        }
    }

    @Override
    public StreamValues generate() {
        CallDetailRecord cdr = new CallDetailRecord();

        cdr.setCallingNumber(pickNumber());
        cdr.setCalledNumber(pickNumber(cdr.getCallingNumber()));
        DateTime dt = DateTime.now().plusMinutes(RandomUtil.randInt(0, 60));

        cdr.setAnswerTime(dt);
        cdr.setCallDuration(RandomUtil.randInt(0, 60 * 5));
        cdr.setCallEstablished(CDRDataGenerator.causeForTermination(errorProb) == CDRDataGenerator.TERMINATION_CAUSE_OK);

        return new StreamValues(cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), cdr);
    }

    private String pickNumber(String excluded) {
        String number = "";
        while (number.isEmpty() || number.equals(excluded)) {
            number = phoneNumbers[rand.nextInt(population)];
        }
        return number;
    }

    private String pickNumber() {
        return phoneNumbers[rand.nextInt(population)];
    }
}
