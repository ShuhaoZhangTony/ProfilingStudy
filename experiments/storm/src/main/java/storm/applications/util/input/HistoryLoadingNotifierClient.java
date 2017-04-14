/**
 * Copyright 2015 Miyuru Dayarathna
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.applications.util.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import static storm.applications.constants.LinearRoadConstants.HISTORY_COMPONENT_HOST;
import static storm.applications.constants.LinearRoadConstants.HISTORY_LOADING_NOTIFIER_PORT;

/**
 * @author miyuru
 *
 */
public class HistoryLoadingNotifierClient {
    //private static Log log = LogFactory.getLog(DailyExpenses.class);

    public static boolean isHistoryLoaded() {
        boolean result = false;

        String host = HISTORY_COMPONENT_HOST;

        try {
            Socket skt = new Socket(host, HISTORY_LOADING_NOTIFIER_PORT);
            PrintWriter out = new PrintWriter(skt.getOutputStream());
            BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));

            out.println("done?");
            out.flush();

            String response = buff.readLine();
            if (response != null) {
                if (response.trim().equals("yes")) {
                    result = true;
                }
            }
            out.close();
            buff.close();
            skt.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static boolean sendRUOK() {
        boolean result = false;
        String host = HISTORY_COMPONENT_HOST;

        try {
            Socket skt = new Socket(host, HISTORY_LOADING_NOTIFIER_PORT);
            PrintWriter out = new PrintWriter(skt.getOutputStream());
            BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));

            out.println("ruok");
            out.flush();

            String response = buff.readLine();
            if (response != null) {
                if (response.trim().equals("imok")) {
                    result = true;
                }
            }
            out.close();
            buff.close();
            skt.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public static void shutdownLoadingNotifier() {
        String host = HISTORY_COMPONENT_HOST;

        try {
            Socket skt = new Socket(host, HISTORY_LOADING_NOTIFIER_PORT);
            PrintWriter out = new PrintWriter(skt.getOutputStream());
            BufferedReader buff = new BufferedReader(new InputStreamReader(skt.getInputStream()));

            out.println("shtdn");
            out.flush();
            out.close();
            buff.close();
            skt.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
