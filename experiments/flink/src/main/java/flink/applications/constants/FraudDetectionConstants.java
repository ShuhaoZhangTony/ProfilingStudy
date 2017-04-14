package flink.applications.constants;

public interface FraudDetectionConstants extends BaseConstants {
    String PREFIX = "fd";
    String DEFAULT_MODEL = "frauddetection/model.txt";

    interface Conf extends BaseConf {
        String PREDICTOR_THREADS = "fd.predictor.threads";
        String PREDICTORSPLIT_THREADS = "fd.predictor.split.threads";
        String PREDICTOR_MODEL = "fd.predictor.model";
        String MARKOV_MODEL_KEY = "fd.markov.model.key";
        String LOCAL_PREDICTOR = "fd.local.predictor";
        String STATE_SEQ_WIN_SIZE = "fd.state.seq.window.size";
        String STATE_ORDINAL = "fd.state.ordinal";
        String DETECTION_ALGO = "fd.detection.algorithm";
        String METRIC_THRESHOLD = "fd.metric.threshold";
    }

    interface Component extends BaseComponent {
        String PREDICTOR = "predictorBolt";
        String PREDICTOR_Split = "predictorBolt_split";
    }

    interface Field {
        String RECORD_KEY = "RECORD_KEY";
        String ENTITY_ID = "entityID";
        String RECORD_DATA = "recordData";
        String SCORE = "score";
        String STATES = "states";
    }

    interface TunedConfiguration {
        int PREDICTOR_THREADS_core1 = 1;
        int PREDICTOR_THREADS_core2 = 1;
        int PREDICTOR_THREADS_core4 = 1;
        int PREDICTOR_THREADS_core8 = 2;
        int PREDICTOR_THREADS_core16 = 1;
        int PREDICTOR_THREADS_core32 = 2;

        int PREDICTOR_THREADS_core8_HP = 1;
        int PREDICTOR_THREADS_core8_Batch = 1;
        int PREDICTOR_THREADS_core32_HP_Batch = 2;
    }
}
