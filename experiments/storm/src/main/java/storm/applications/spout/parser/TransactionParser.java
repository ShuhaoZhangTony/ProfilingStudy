package storm.applications.spout.parser;

import com.google.common.collect.ImmutableList;
import storm.applications.util.stream.StreamValues;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TransactionParser extends Parser {

    @Override
    public List<StreamValues> parse(String input) {
        String[] items = input.split(",", 2);
        return ImmutableList.of(new StreamValues(items[0], items[1]));
    }

    @Override
    public List<StreamValues> parse(String[] input) {

        //String[][] items = new String[2][input.length];

        LinkedList<StreamValues> items = new LinkedList<>();
        for (int i = 0; i < input.length; i++) {
            String[] temp = input[i].split(",", 2);//first dimension as usual. second dimension column reflects batch index
            StreamValues v = new StreamValues(temp);
            items.add(v);
        }
        return items;
    }
}
