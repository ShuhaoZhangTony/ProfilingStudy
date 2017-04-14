package flink.applications.util;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;
import org.apache.storm.guava.collect.HashMultimap;
import org.apache.storm.guava.collect.Multimap;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by szhang026 on 5/3/2016.
 */
public class Multi_Key_value_Map<V> {

    public Multimap cache;
    //private HashMap<String, Integer> _index = new HashMap<String, Integer>();

    public Multi_Key_value_Map() {
        cache = HashMultimap.create();
    }

    private String MergeKey(String... key) {

        StringBuilder sb = new StringBuilder();

        for (String tempString : key) {
            sb.append(tempString).append(",");
        }


//        String newkey = key[0];
//        for (int i = 1; i < key.length; i++)
//            newkey = newkey.concat(" " + key[i]);
        return sb.toString();
    }

    public void put(int worker, V value, String... key) {
        if (key.length > 0) {
            long newkey = MergeKey(key).hashCode() & 0x7fffffff % worker;
            cache.put(newkey, value);
        } else
            cache.put(0l, value);//default key=0
        //return newkey;
    }

    public Collection<V> get(int worker, String... key) {
        int newkey = MergeKey(key).hashCode() % worker;
        return cache.removeAll(newkey);

    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public void emit(String streamID, OutputCollector collector) {
        Set keySet = cache.keySet();
        Iterator keyIterator = keySet.iterator();
//        if (keySet.size() > 2) {
//            System.out.println("Wrong!cache is not cleaned");
//            System.exit(-1);
//        }
        while (keyIterator.hasNext()) {
            long key = (long) keyIterator.next();
            Collection values = cache.get(key);
            LinkedList e = new LinkedList(values);
            collector.emit(streamID,
                    new Values(e, key));
        }
    }

    public void emit(String streamID, SpoutOutputCollector collector, String msgID) {
        Set keySet = cache.keySet();
        Iterator keyIterator = keySet.iterator();
//        if (keySet.size() > 2) {
//            System.out.println("Wrong!cache is not cleaned");
//            System.exit(-1);
//        }
        while (keyIterator.hasNext()) {
            long key = (long) keyIterator.next();
            Collection values = cache.get(key);
            LinkedList e = new LinkedList(values);
            collector.emit(streamID,
                    new Values(e, key), msgID);
        }
    }
}
