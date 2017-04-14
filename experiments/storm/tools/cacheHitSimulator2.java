import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by szhang026 on 4/23/2016.
 */
public class cacheHitSimulator2 {
    static String path = "C:\\Users\\szhang026\\Documents\\Profile-experiments\\storm-tracing\\log\\genlib";
    private static int start = 0;
    private static int cnt = 0;
    private static int app;

    private static double nextTime(double rateParameter) {
        return -Math.log(1.0 - Math.random()) / rateParameter;
    }

    private static int getsum(Collection<String> l, Map<String, Integer> instruction_per_function) {
        int sum = 0;
        for (String i : l) {
            sum += instruction_per_function.get(i);
        }
        return sum;
    }


    private static LinkedList<record> clean_InTrace_results(int app) throws IOException {
        PrintWriter writer = null;
        Scanner sc = null;
        switch (app) {
            case 3: {//analysis wc
                sc = new Scanner(new File(path + "\\test.trace"));

                break;
            }
            case 4: {//analysis wc
                sc = new Scanner(new File(path + "\\wc.trace"));
                break;
            }
            case 5: {//analysis fd
                sc = new Scanner(new File(path + "\\fd.trace"));
                break;
            }
            case 6: {//analysis lg

                sc = new Scanner(new File(path + "\\lg.trace"));

                break;
            }
            case 7: {//analysis sd

                sc = new Scanner(new File(path + "\\sd.trace"));

                break;
            }
            case 8: {//analysis vs

                sc = new Scanner(new File(path + "\\vs.trace"));

                break;
            }
            case 9: {//analysis tm

                sc = new Scanner(new File(path + "\\tm.trace"));

                break;
            }
            case 10: {//analysis lr

                sc = new Scanner(new File(path + "\\lr.trace"));

                break;
            }
        }

        File file = new File(path + "\\" + app + "_gaps.txt");
        file.getParentFile().mkdirs();
        writer = new PrintWriter(new FileWriter(file, false), true);

        String r;
        String[] r_a;
        String function;
        LinkedList<record> list = new LinkedList<record>();
        String pre1 = "", pre2 = "", pre3 = "";
        while (sc.hasNextLine()) {
            String read = sc.nextLine().trim();
            String[] read_s = read.split(" ");
            if (read_s.length == 4) {

                int consequtive_exe = Integer.parseInt(read_s[0]) / 8000;
                for (int i = 0; i < consequtive_exe; i++) {
                    list.add(new record(8000, pre1, pre2, pre3));
                    pre1 = read_s[1];
                    pre2 = read_s[2].concat(String.valueOf(i));
                    pre3 = read_s[3];

                    writer.println(8000 * 4);
                }
                list.add(new record(Integer.parseInt(read_s[0]) % 8000, pre1, pre2, pre3));
                pre1 = read_s[1];
                pre2 = read_s[2];
                pre3 = read_s[3];

                writer.println(Integer.parseInt(read_s[0]) % 8000 * 4);
                writer.flush();
            }
        }
        writer.close();
        list.removeFirst();
        return list;
    }

    private static class record {
        /*
        * Column Labels:
            PTT_INSTS  :: Per Thread Instructions
            EN_EX      :: Method Enter or Method Exit
            NAME_M     :: Name of Method
            NAME_T     :: Name or Thread
        * */
        public int PTT_INSTS;
        public int EN_EX;//0 means enter, 1 means exit
        public String NAME_M;
        public String NAME_T;


        public record(int read_0, String read_1, String read_2, String read_3) {
            PTT_INSTS = read_0;

            if (read_1.equalsIgnoreCase("<")) {
                EN_EX = 1;
            } else {
                EN_EX = 0;
            }
            NAME_M = read_2;
            NAME_T = read_3;
        }

    }

    public static void main(String[] arg) throws IOException {
        app = Integer.parseInt(arg[0]);
        // for (int app = app_start; app < app_end; app++) {

        LinkedList<record> record_trace = clean_InTrace_results(app);
        LinkedList<String> event_trace = new LinkedList<>();
        Map<String, Integer> Instruction_per_function = new HashMap<>();
        Map<String, Integer> Appears_per_function = new HashMap<>();

        Iterator<record> tr = record_trace.iterator();

        while (tr.hasNext()) {
            record i = tr.next();
            int scale = 1;
            int triped_inst = i.PTT_INSTS < scale ? 1 : (int) (i.PTT_INSTS / (double) scale) * scale;
            int size_per_instruction = 4;//4 bytes
            String function = i.NAME_M.concat(String.valueOf(triped_inst));
            if (i.EN_EX == 0) {
                event_trace.add(function);
                if (!Appears_per_function.containsKey(function)) {
                    Appears_per_function.put(function, 1);
                } else {
                    Appears_per_function.put(function, Appears_per_function.get(function) + 1);
                }
            }
            if (!Instruction_per_function.containsKey(function)) {
                Instruction_per_function.put(function, triped_inst * size_per_instruction);
            }
            tr.remove();
        }
    }
}