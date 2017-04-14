package storm.applications.grouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class HSP_shuffleGrouping implements CustomStreamGrouping {


    private WorkerTopologyContext _ctx;
    private GlobalStreamId _stream;
    private List<Integer> _targetTasks;
    private List<Integer> _targetTasks_counter;
    private int index = 0;
    private int num_senders;
    private boolean read_configuration=true;
    transient private Map<Integer, List<Double>> partition_ratio;
    transient private Map<Integer, Map<Integer,Integer>> achieved_ratio;
    transient private Map<Integer, Integer> sender_index;
    public HSP_shuffleGrouping(int num_senders, int num_receivers, LinkedList<Double> partition_ratio) {
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            // create string writer
            fw = new FileWriter(new File("num_senders.txt"),false);
            //create buffered writer
            bw = new BufferedWriter(fw);
            bw.write(String.valueOf(num_senders));
            bw.flush();
            bw.close();

            for (int i = 0; i < num_senders; i++) {
                // create string writer
                fw = new FileWriter(new File(String.valueOf(i).concat(".txt")),false);
                //create buffered writer
                bw = new BufferedWriter(fw);
                bw.write(String.valueOf(partition_ratio.subList(i * num_receivers, (i + 1) * num_receivers)));
                bw.flush();
                bw.close();
            }
        } catch (IOException e) {
            // if I/O error occurs
            e.printStackTrace();
        }
    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _ctx = context;
        _stream = stream;
        _targetTasks = targetTasks;//have several spliters
        this.partition_ratio=new HashMap<>();
        this.sender_index=new HashMap<>();
        this.achieved_ratio=new HashMap<>();

        try {
            this.num_senders=new Scanner(new File("num_senders.txt")).nextInt();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for(int i=0;i<this.num_senders;i++) {

            List<Double> partial_ratio=new LinkedList<>();
            try {
                Scanner sc=new Scanner(new File(String.valueOf(i).concat(".txt")));
                String read=sc.nextLine().replaceAll("\\[","").replaceAll("]","");
              //  while(sc.hasNext())
                String[] split = read.split(",");
                for(String str: split)
                partial_ratio.add(Double.valueOf(str));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            this.partition_ratio.put(i, partial_ratio);//the key should be senderID, but it has not been initialized yet.
            this.achieved_ratio.put(i,new HashMap<>());
            Map<Integer, Integer> integerIntegerMap = this.achieved_ratio.get(i);
            for(int s=0;s<partial_ratio.size();s++) {
                integerIntegerMap.put(s,0);//Haven't deliver anything yet.
            }
        }
    }
//    public int read_statstics(int index) throws IOException {
//        File f = new File("split" + index + ".txt");
//        FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
//
//        MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
//        CharBuffer charBuf = b.asCharBuffer();
//
//        int rt = charBuf.get();
//        return rt;
//    }

    //senderID: who send this information.
    @Override
    public List<Integer> chooseTasks(int senderID, List<Object> values) {

        if(read_configuration) {
            this._ctx.getComponentId(senderID);//this returns the name of component that's doing the stream partition.
            Map<Integer, String> map = this._ctx.getTaskToComponent();
            Set<Integer> keysByValue = getKeysByValue(map, this._ctx.getComponentId(senderID));
            int cnt=0;
            for(Integer i: keysByValue) {
                this.sender_index.put(i, cnt);
            }

            read_configuration=false;
        }
        index = index > 0 ? 0 : 1;

        return Arrays.asList(_targetTasks.get(index));
    }

    public static <T, E> Set<T> getKeysByValue(Map<T, E> map, E value) {
        return map.entrySet()
                .stream()
                .filter(entry -> Objects.equals(entry.getValue(), value))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }


}

