package flink.applications.bolt;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.WordCountConstants.Field;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class SplitSentenceBolt_org extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt_org.class);
    private static final String splitregex = " ";
    long start = 0, end = 0;
    boolean update = false;
    private int executionLatency = 0;
    private int curr = 0, precurr = 0;

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    protected void update_statistics(int number_ofExecuted) throws IOException {

        File f = new File("split" + this.context.getThisTaskId() + ".txt");

        FileChannel channel = FileChannel.open(f.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

        MappedByteBuffer b = channel.map(FileChannel.MapMode.READ_WRITE, 0, 4096);
        CharBuffer charBuf = b.asCharBuffer();

        char[] string = String.valueOf(number_ofExecuted).toCharArray();
        charBuf.put(string);

        //System.out.println("Waiting for client.");
        //while (charBuf.get(0) != '\0') ;
        //System.out.println("Finished waiting.");
    }

    @Override
    public void execute(Tuple input) {
        String[] words = input.getString(0).split(splitregex);
        for (String word : words) {
            if (!StringUtils.isBlank(word))
                collector.emit(input, new Values(word));
        }
        collector.ack(input);
        //  curr++;
 /*       curr++;  	         
        if(curr==1){
    		File theDir = new File(config.getString("metrics.output")+"\\split");
			// if the directory does not exist, create it
			if (!theDir.exists()) {
			    //System.out.println("creating directory: " + theDir);
			    boolean result = false;

			    try{
			        theDir.mkdir();
			        result = true;
			    } 
			    catch(SecurityException se){
			        //handle it
			    }        
//			    if(result) {    
//			        System.out.println("DIR created");  
//			    }
			}
    		start=System.nanoTime();
        	FileWriter fw;
			try {
				fw = new FileWriter(new File(config.getString("metrics.output")+"\\split\\"+context.getThisTaskId()+"split.txt"));
				writer = new BufferedWriter(fw);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			total_thread=config.getInt(Conf.SPLITTER_THREADS);
    	}    	    
    	if(curr%(10000/total_thread)==0){
    		end=System.nanoTime();    		
    		try {
    			writer.write(((end-start)/1000.0)/1000000.0+"\t"+curr+"\n");
				writer.flush();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		//logger.add(((end-start)/1000.0)/1000000.0+"\t"+curr+"\n");
    		//LOG
    		//System.out.println("Time spend in split: "+(end-start)/1000.0+" ms"+"total emited from split:"+curr);
    		//start=System.nanoTime();
    	}
*/
//    	if(curr==1000000){//last element, 1M
////    		end=System.nanoTime();    		
////    		System.out.println(((end-start)/1000.0)/1000000.0+"\t"+curr+"\n");
////    		try {
////    			writer.write(((end-start)/1000.0)/1000000.0+"\t"+curr+"\n");
////				writer.flush();
////				writer.close();
////				
////			} catch (IOException e) {
////				// TODO Auto-generated catch block
////				e.printStackTrace();
////			}
////    		 try {
////				TimeUnit.SECONDS.sleep(5);
////			} catch (InterruptedException e) {
////				// TODO Auto-generated catch block
////				e.printStackTrace();
////			}
//    		     
//     		try {
//     			int length=logger.size();
//     			for(int i=0;i<length;i++){
//     				writer.write(logger.poll());     				
//     			}
//     			writer.flush();
// 				writer.close();
// 				
// 			} catch (IOException e) {
// 				// TODO Auto-generated catch block
// 				e.printStackTrace();
// 			} 
//    		 
////   		 try {
////				TimeUnit.SECONDS.sleep(3000);
////			} catch (InterruptedException e) {
////				// TODO Auto-generated catch block
////				e.printStackTrace();
////			}	 
//    	}    	    
//    	if(curr==999999){
//    		end=System.nanoTime();    		
//    		try {
//    			writer.write("Split stop, Total time spend in split: "+((end-start)/1000.0)/1000000.0+" s\t"+"total executed from split:"+curr+"\n");
//				writer.flush();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//    	}    	    
    }
//    public void execute(Tuple input) {
//        if (TupleUtils.isTickTuple(input)) {
//            try {
//                update_statistics(curr-precurr);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            precurr=curr;
//        } else {
//            String[] words = input.getString(0).split(splitregex);
//            for (String word : words) {
//                if (!StringUtils.isBlank(word))
//                    collector.emit(input, new Values(word));
//                collector.ack(input);
//            }
//        }
//        curr++;  	    
//    }

}
