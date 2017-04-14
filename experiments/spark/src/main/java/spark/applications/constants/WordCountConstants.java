package spark.applications.constants;

public interface WordCountConstants extends BaseConstants {
    String PREFIX = "wc";

    interface Field {
        String TEXT = "text";
        String WORD = "word";
        String COUNT = "count";
        String LargeData = "LD";
    }

    interface Conf extends BaseConf {
        String SPLITTER_THREADS = "wc.splitter.threads";
        String COUNTER_THREADS = "wc.counter.threads";
    }

    interface Component extends BaseComponent {
        String SPLITTER = "splitSentence";
        String COUNTER = "wordCount";
    }

    interface TunedConfiguration {
        int Splitter_core1 = 1;
        int Counter_core1 = 1;

        int Splitter_core2 = 1;
        int Counter_core2 = 1;

        int Splitter_core4 = 1;
        int Counter_core4 = 1;

        int Splitter_core8 = 1;
        int Counter_core8 = 1;

        int Splitter_core16 = 1;
        int Counter_core16 = 1;

        int Splitter_core32 = 1;
        int Counter_core32 = 1;

        int Splitter_core8_HP = 1;
        int Counter_core8_HP = 1;

        int Splitter_core8_Batch = 1;
        int Counter_core8_Batch = 1;

        int Splitter_core32_HP_Batch = 1;
        int Counter_core32_HP_Batch = 1;
    }
}
