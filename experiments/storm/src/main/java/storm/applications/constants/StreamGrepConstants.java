package storm.applications.constants;

public interface StreamGrepConstants extends BaseConstants {
    String PREFIX = "sg";

    interface Field {
        String TEXT = "text";
    }

    interface Conf extends BaseConf {
        String StreamGrepBoltThreads = "sg.StreamGrepBolt.threads";
    }

    interface Component extends BaseComponent {
        String GREP = "StreamingGrep";
    }
}
