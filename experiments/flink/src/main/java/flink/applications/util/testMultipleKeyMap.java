package flink.applications.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by szhang026 on 5/3/2016.
 */
public class testMultipleKeyMap {
    public static void main(String[] arg) {

        Multi_Key_value_Map mymap = new Multi_Key_value_Map();
        mymap.put(1, "A", "A", "A");
        mymap.put(2, "A", "A", "A");
        mymap.put(2, "A", "A", "B");
        // while (!mymap.isEmpty()) {
        Set keySet = mymap.cache.keySet();
        Iterator keyIterator = keySet.iterator();

        while (keyIterator.hasNext()) {
            String key = (String) keyIterator.next();
            Collection values = mymap.cache.get(key);
            //Collection c = mymap.get("A", "A", "A");
            LinkedList e = new LinkedList(values);
        }

        //  }


        mymap.get(1, "A", "A", "B");

    }
}
