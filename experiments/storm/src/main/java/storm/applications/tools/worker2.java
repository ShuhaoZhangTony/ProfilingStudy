package storm.applications.tools;

/**
 * Created by szhang026 on 5/30/2016.
 */
public class worker2 {
    class Control {
        public volatile boolean flag = false;
    }
    final Control control = new Control();

    class mapMatcher implements Runnable {
        @Override
        public void run() {
            while ( !control.flag ) {

            }
        }
    }

    class speedCal implements Runnable {
        @Override
        public void run() {
            while ( !control.flag ) {

            }
        }
    }

    private void test() {
        mapMatcher main = new mapMatcher();
        speedCal help = new speedCal();

        new Thread(main).start();
        new Thread(help).start();
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            worker2 test = new worker2();
            test.test();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}