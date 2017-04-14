package storm.applications.bolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.bolt.base.AbstractBolt;
import storm.applications.constants.WordCountConstants.Conf;
import storm.applications.constants.WordCountConstants.Field;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class WordCountBolt_communicate extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountBolt_communicate.class);
    private static final String splitregex = " ";
    private static LinkedList<String> logger = new LinkedList<String>();
    private final Map<String, MutableLong> counts = new HashMap<>();
    long start = 0, end = 0, curr = 0;
    private boolean print = false;
    transient private BufferedWriter writer;
    private int total_thread = 0;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.LargeData);
    }

    //private MutableLong[]array=new MutableLong[20000];
    @Override
    public void execute(Tuple input) {
     //   curr++;
    }
}
