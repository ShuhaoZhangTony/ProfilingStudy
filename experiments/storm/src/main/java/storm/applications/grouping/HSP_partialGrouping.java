package storm.applications.grouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HSP_partialGrouping implements CustomStreamGrouping {

    private Map _map;
    private WorkerTopologyContext _ctx;
    private GlobalStreamId _stream;
    private List<Integer> _targetTasks;
    private int index = 0;

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        _ctx = context;
        _stream = stream;
        _targetTasks = targetTasks;
    }

    public int read_statstics(int index) throws IOException {
        File f = new File("split" + index + ".txt");
        FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        CharBuffer charBuf = b.asCharBuffer();

        int rt = charBuf.get();
        // Prints 'Hello server'
//        char c;
//        while( ( c = charBuf.get() ) != 0 ) {
//            System.out.print( c );
//        }
        return rt;
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        for (int i = 0; i < _targetTasks.size(); i++) {
            try {
                System.out.println("Im thread:" + _targetTasks.get(index) + ", in last 10 seconds, I have processed " + read_statstics(_targetTasks.get(i)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //Long groupingKey = Long.valueOf( values.get(0).toString().charAt(0));
        index = index > 0 ? 0 : 1;
        return Arrays.asList(_targetTasks.get(index));
    }
}

