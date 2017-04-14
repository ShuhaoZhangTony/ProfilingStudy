package storm.applications.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.applications.util.event.ExpenditureEvent;
import storm.applications.util.event.HistoryEvent;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created by szhang026 on 21/2/2016.
 */
public class DailyExpensesBolt extends storm.applications.bolt.base.AbstractBolt {
    private LinkedList<ExpenditureEvent> expEvtList;
    private LinkedList<HistoryEvent> historyEvtList;

    public DailyExpensesBolt() {
        expEvtList = new LinkedList<ExpenditureEvent>();
        historyEvtList = new LinkedList<HistoryEvent>();
    }

    @Override
    public Fields getDefaultFields() {
        setFields("dailyexp_events", new Fields("vid",
                "sum"));
        return new Fields("vid",
                "sum");
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        super.prepare(conf, context, collector);

        String historyFile = config.getString("linear-history-file");
        loadHistoricalInfo(historyFile);
        //String historyFile = "C:\\Users\\szhang026\\Documents\\data/historical-tolls10.out";//This is also for the moment. Have to implement a concrete solution later.


        // HistoryLoadingNotifier notifierObj = new HistoryLoadingNotifier(false);
        // notifierObj.start();//The notification server starts at this point


        // notifierObj.setStatus(true);//At this moment we notify all the listeners that we have done loading the history data
    }

    @Override
    public void execute(Tuple tuple) {
        ExpenditureEvent expEvt = new ExpenditureEvent();
        expEvt.time = tuple.getLong(0);
        expEvt.vid = tuple.getInteger(1);
        expEvt.xWay = tuple.getByte(2);
        expEvt.qid = tuple.getInteger(3);
        expEvt.day = tuple.getInteger(4);

        process(expEvt);
        collector.ack(tuple);
    }
//    @Override
//    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declareStream("dailyexp_events", new Fields(	"vid",
//                "sum"));
//    }

    public void process(ExpenditureEvent evt) {
        int len = 0;
        Statement stmt;

        Iterator<HistoryEvent> itr = historyEvtList.iterator();
        int sum = 0;
        while (itr.hasNext()) {
            HistoryEvent histEvt = (HistoryEvent) itr.next();

            if ((histEvt.carid == evt.vid) && (histEvt.x == evt.xWay) && (histEvt.d == evt.day)) {
                sum += histEvt.daily_exp;
            }
        }

//		try{
//		    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
//
//		    bytesMessage.writeBytes((Constants.DAILY_EXP_EVENT_TYPE + " " + evt.vid + " " + sum).getBytes());
//		    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//		    producer_output.send(bytesMessage);
//		}catch(JMSException e){
//			e.printStackTrace();
//		}

        collector.emit("dailyexp_events", new Values(evt.vid, sum));

    }

    public void loadHistoricalInfo(String inputFileHistory) {
        BufferedReader in;
        try {
            in = new BufferedReader(new FileReader(inputFileHistory));

            String line;
            int counter = 0;
            int batchCounter = 0;
            int BATCH_LEN = 10000;//A batch size of 1000 to 10000 is usually OK
            Statement stmt;
            StringBuilder builder = new StringBuilder();

            //log.info(Utilities.getTimeStamp() + " : Loading history data");
            while ((line = in.readLine()) != null) {

//					try{
//						Thread.sleep(1000);//just wait one second and check again
//					}catch(InterruptedException e){
//						//Just ignore
//					}

                //#(1 8 0 55)
                    /*
					0 - Car ID
					1 - day
					2 - x - Expressway number
					3 - daily expenditure
					*/

                String[] fields = line.split(" ");
                fields[0] = fields[0].substring(2);
                fields[3] = fields[3].substring(0, fields[3].length() - 1);

                historyEvtList.add(new HistoryEvent(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3])));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //log.info(Utilities.getTimeStamp() + " : Done Loading history data");
        //Just notfy this to the input event injector so that it can start the data emission process
//        try {
//            PrintWriter writer = new PrintWriter("done.txt", "UTF-8");
//            writer.println("\n");
//            writer.flush();
//            writer.close();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }

    }

}
