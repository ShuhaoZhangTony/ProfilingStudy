package flink.applications.util;

import flink.applications.constants.BaseConstants;
import flink.applications.util.config.Configuration;
import flink.applications.util.geoip.IPLocation;
import flink.applications.util.geoip.IPLocationFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by szhang026 on 11/10/2015.
 */
public class CleanData {
    public static final String IP = "ip";
    public static final String TIMESTAMP = "timestamp";
    public static final String REQUEST = "request";
    public static final String RESPONSE = "response";
    public static final String BYTE_SIZE = "byte_size";
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
    private static final int NUM_FIELDS = 8;
    private static int index_e = 0;
    private static int end_index;
    private static String[] array;
    private static BufferedWriter writer;
    private static IPLocation resolver;

    public static void openFile(String fileName, List<String> str_l) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(fileName));
        while (scanner.hasNextLine()) {
            str_l.add(scanner.nextLine()); //normal..
        }

        scanner.close();
        array = str_l.toArray(new String[str_l.size()]);
        end_index = array.length;
    }

    public static void main(String[] args) throws IOException {

        String value = null;
        List<String> str_l = new LinkedList<String>();
        openFile("C:\\\\Users\\\\szhang026\\\\Documents\\\\NUMA-Storm/data/http-server.log", str_l);

        Configuration conf = new Configuration();
        conf.put(BaseConstants.BaseConf.GEOIP2_DB, "C:\\\\Users\\\\szhang026\\\\Documents\\\\NUMA-Storm/data/GeoLite2-City.mmdb");
        resolver = IPLocationFactory.create("geoip2", conf);
        FileWriter fw;
        try {
            fw = new FileWriter(new File("C:\\\\Users\\\\szhang026\\\\Documents\\\\NUMA-Storm/data/http-server_cleaned.log"));
            writer = new BufferedWriter(fw);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        while (index_e < end_index) {
            value = array[index_e % array.length];
            if (parseLine(value)) {
                String randomIP = null;
                while (resolver.resolve(randomIP) == null) {
                    Random r = new Random();
                    randomIP = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
                }

                String[] res = value.split("-");

                String result = randomIP.concat(" - -").concat(res[2]);
                if (parseLine(result))
                    writer.write(result + "\n");
            }
            index_e++;
        }
        writer.flush();
        writer.close();
    }

    public static boolean parseLine(String logLine) {
        Map<String, Object> entry = new HashMap<>();
        String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)";

        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logLine);

        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            return false;
        }
        entry.put(IP, matcher.group(1));
        entry.put(TIMESTAMP, dtFormatter.parseDateTime(matcher.group(4)).toDate());
        entry.put(REQUEST, matcher.group(5));
        entry.put(RESPONSE, Integer.parseInt(matcher.group(6)));

        if (matcher.group(7).equals("-"))
            entry.put(BYTE_SIZE, 0);
        else
            entry.put(BYTE_SIZE, Integer.parseInt(matcher.group(7)));


        return true;
    }

}
