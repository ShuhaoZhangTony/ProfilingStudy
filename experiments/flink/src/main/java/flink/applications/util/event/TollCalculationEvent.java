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
public class TollCalculationEvent {

    public int vid; //vehicle identifier
    public int toll; //The toll
    public byte segment; //The mile

    public TollCalculationEvent() {

    }

    public int getVid() {
        return vid;
    }

    public void setVid(int vid) {
        this.vid = vid;
    }

    public int getToll() {
        return toll;
    }

    public void setToll(int toll) {
        this.toll = toll;
    }

    public byte getSegment() {
        return segment;
    }

    public void setSegment(byte segment) {
        this.segment = segment;
    }

    @Override
    public String toString() {
        return "TollCalculationEvent [vid=" + vid + ", toll=" + toll + ", segment=" + segment + "]";
    }

    public String toCompressedString() {
        return "" + vid + " " + toll + " " + segment;
    }
}
