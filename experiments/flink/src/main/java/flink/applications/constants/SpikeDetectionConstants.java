package flink.applications.constants;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface SpikeDetectionConstants extends BaseConstants {
    String PREFIX = "sd";

    interface Conf extends BaseConf {
        String PARSER_VALUE_FIELD = "sd.parser.value_field";
        String GENERATOR_COUNT = "sd.generator.count";
        String MOVING_AVERAGE_THREADS = "sd.moving_average.threads";
        String MOVING_AVERAGE_WINDOW = "sd.moving_average.window";
        String SPIKE_DETECTOR_THREADS = "sd.spike_detector.threads";
        String SPIKE_DETECTOR_THRESHOLD = "sd.spike_detector.threshold";
    }

    interface Field {
        String DEVICE_ID = "deviceID";
        String TIMESTAMP = "timestamp";
        String VALUE = "value";
        String MOVING_AVG = "movingAverage";
        String MESSAGE = "message";
    }

    interface Component extends BaseComponent {
        String MOVING_AVERAGE = "movingAverageBolt";
        String SPIKE_DETECTOR = "spikeDetectorBolt";
    }

    interface TunedConfiguration {
        int MOVING_AVERAGE_THREADS_core1 = 1;
        int SPIKE_DETECTOR_THREADS_core1 = 1;
        int MOVING_AVERAGE_THREADS_core2 = 1;
        int SPIKE_DETECTOR_THREADS_core2 = 1;
        int MOVING_AVERAGE_THREADS_core4 = 1;
        int SPIKE_DETECTOR_THREADS_core4 = 1;
        int MOVING_AVERAGE_THREADS_core8 = 2;
        int SPIKE_DETECTOR_THREADS_core8 = 1;
        int MOVING_AVERAGE_THREADS_core16 = 1;
        int SPIKE_DETECTOR_THREADS_core16 = 1;
        int MOVING_AVERAGE_THREADS_core32 = 16;
        int SPIKE_DETECTOR_THREADS_core32 = 16;

        int MOVING_AVERAGE_THREADS_core8_HP = 1;
        int SPIKE_DETECTOR_THREADS_core8_HP = 1;
        int MOVING_AVERAGE_THREADS_core8_Batch = 1;
        int SPIKE_DETECTOR_THREADS_core8_Batch = 1;
        int MOVING_AVERAGE_THREADS_core32_HP_Batch = 16;
        int SPIKE_DETECTOR_THREADS_core32_HP_Batch = 8;

    }
}
