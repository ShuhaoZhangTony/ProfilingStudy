package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.MutableLong;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.WordCountConstants.Conf;
import flink.applications.constants.WordCountConstants.Field;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        curr++;
//    	if(curr==1){
//    		 for(int i=0;i<20000;i++){        	
//    	        	array[i]= new MutableLong(i);
//    	        }    		
//    	}
    }

}
