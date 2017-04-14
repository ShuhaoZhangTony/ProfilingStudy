package flink.applications.util.config;

import flink.applications.constants.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szhang026 on 5/9/2016.
 */

public class load_TunedConfiguration {

    private String cfg;


    public load_TunedConfiguration(String config) {
        this.cfg = config;
    }

    private static List<String> changeValueOf(List<String> lines, String username, int newVal) {
        List<String> newLines = new ArrayList<String>();
        for (String line : lines) {
            if (line.contains(username)) {
                String[] vals = line.split("=");
                newLines.add(vals[0] + "=" + String.valueOf(newVal));
            } else {
                newLines.add(line);
            }

        }
        return newLines;
    }


    public void replaceSelected(String config, String target, int value) {
        File f = new File(config);
        f.setWritable(true);
        try {
            List<String> lines = Files.readAllLines(f.toPath(), Charset.forName("UTF-8"));
            Files.write(f.toPath(), changeValueOf(lines, target, value), Charset.forName("UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void load_TunedConfiguration_1core(String _application) {

        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core1);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core1);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core1);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core1);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core1);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core1);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core1);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core1);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core1);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core1);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core1);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core1);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core1);
                break;
            }
        }
    }

    public void load_TunedConfiguration_2core(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core2);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core2);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core2);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core2);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core2);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core2);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core2);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core2);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core2);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core2);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core2);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core2);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core2);
                break;
            }
        }
    }

    public void load_TunedConfiguration_4core(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core4);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core4);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core4);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core4);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core4);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core4);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core4);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core4);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core4);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core4);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core4);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core4);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core4);
                break;
            }
        }
    }

    public void load_TunedConfiguration_8core(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8);
                break;
            }
        }
    }

    public void load_TunedConfiguration_16core(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core16);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core16);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core16);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core16);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core16);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core16);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core16);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core16);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core16);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core16);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core16);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core16);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core16);
                break;
            }
        }
    }

    public void load_TunedConfiguration_32core(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core32);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core32);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core32);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core32);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core32);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core32);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core32);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core32);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core32);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core32);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core32);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core32);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core32);
                break;
            }
        }
    }


    public void load_TunedConfiguration_8core_Batch(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8_Batch);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8_Batch);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8_Batch);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8_Batch);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8_Batch);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8_Batch);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8_Batch);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8_Batch);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8_Batch);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8_Batch);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8_Batch);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8_Batch);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8_Batch);
                break;
            }
        }
    }

    public void load_TunedConfiguration_8core_HP(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8_HP);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8_HP);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8_HP);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8_HP);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8_HP);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8_HP);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8_HP);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8_HP);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8_HP);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8_HP);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8_HP);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8_HP);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8_HP);
                break;
            }
        }
    }

    public void load_TunedConfiguration_32core_HP_Batch(String _application) {
        switch (_application) {
            case "word-count": {
                replaceSelected(cfg, WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core32_HP_Batch);
                replaceSelected(cfg, WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core32_HP_Batch);
                break;
            }
            case "fraud-detection": {
                replaceSelected(cfg, FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core32_HP_Batch);
                break;
            }
            case "log-processing": {
                replaceSelected(cfg, LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core32_HP_Batch);

                break;
            }
            case "spike-detection": {
                replaceSelected(cfg, SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core32_HP_Batch);
                replaceSelected(cfg, SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core32_HP_Batch);
                break;
            }
            case "voipstream": {
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core32_HP_Batch);
                replaceSelected(cfg, VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core32_HP_Batch);
                break;
            }
            case "traffic-monitoring": {
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core32_HP_Batch);
                replaceSelected(cfg, TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core32_HP_Batch);
                break;
            }
            case "linear-road-full": {
                replaceSelected(cfg, LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core32_HP_Batch);
                replaceSelected(cfg, LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core32_HP_Batch);
                replaceSelected(cfg, LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core32_HP_Batch);
                break;
            }
        }
    }
}
