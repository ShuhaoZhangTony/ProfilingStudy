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

package flink.applications.util.event;

/**
 * @author miyuru
 */
public class AccountBalanceEvent {

    public long time; //A timestamp measured in seconds since the start of the simulation
    public int vid; //Vehicle identifier
    public int qid; //Query ID

    public AccountBalanceEvent(String[] fields) {
        this.time = Long.parseLong(fields[1]);//Seconds since start of simulation
        this.vid = Integer.parseInt(fields[2]);//Car ID
        this.qid = Integer.parseInt(fields[9]);//Query ID
    }

    public AccountBalanceEvent(long ttime, int tvid, int tqid) {
        this.time = ttime;//Seconds since start of simulation
        this.vid = tvid;//Car ID
        this.qid = tqid;//Query ID
    }

    @Override
    public String toString() {
        return "AccountBalanceEvent [time=" + time + ", vid=" + vid + ", qid="
                + qid + "]";
    }
}
