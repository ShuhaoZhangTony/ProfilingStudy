package storm.applications.util.config;

import org.apache.storm.Config;
import storm.applications.constants.*;

/**
 * Created by szhang026 on 5/9/2016.
 */
public class load_TunedConfiguration {

    private Config config;

    public load_TunedConfiguration(Config config) {
        this.config = config;
    }

    public void load_TunedConfiguration_1core(String _application) {

        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core1);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core1);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core1);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core1);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core1);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core1);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core1);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core1);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core1);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core1);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core1);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core1);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core1);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core1);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core1);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core1);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core1);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core1);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core1);
                break;
            }
        }
    }

    public void load_TunedConfiguration_2core(String _application) {

        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core2);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core2);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core2);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core2);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core2);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core2);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core2);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core2);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core2);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core2);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core2);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core2);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core2);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core2);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core2);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core2);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core2);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core2);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core2);
                break;
            }
        }
    }

    public void load_TunedConfiguration_4core(String _application) {

        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core4);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core4);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core4);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core4);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core4);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core4);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core4);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core4);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core4);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core4);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core4);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core4);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core4);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core4);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core4);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core4);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core4);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core4);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core4);
                break;
            }
        }
    }

    public void load_TunedConfiguration_8core(String _application) {

        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core8);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core8);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core8);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core8);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core8);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core8);
                break;
            }
        }
    }

    public void load_TunedConfiguration_16core(String _application) {
        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core16);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core16);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core16);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core16);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core16);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core16);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core16);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core16);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core16);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core16);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core16);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core16);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core16);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core16);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core16);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core16);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core16);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core16);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core16);
                break;
            }
        }

    }

    public void load_TunedConfiguration_32core(String _application) {

        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core32);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core32);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core32);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core32);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core32);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core32);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core32);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core32);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core32);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core32);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core32);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core32);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core32);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core32);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core32);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core32);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core32);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core32);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core32);
                break;
            }
        }

    }


    public void load_TunedConfiguration_8core_Batch(String _application, int _batch) {
        switch (_application) {
            case "word-count": {
                switch (_batch) {
                    case 2: {
                        config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8_Batch2);
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8_Batch2);
                        config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core8_Batch2);
                        break;
                    }
                    case 4: {
                        config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8_Batch4);
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8_Batch4);
                        config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core8_Batch4);
                        break;
                    }
                    case 8: {
                        config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8_Batch8);
                        config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8_Batch8);
                        config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core8_Batch8);
                        break;
                    }
                }
            }
            case "fraud-detection": {
                switch (_batch) {
                    case 2: {
                        config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8_Batch2);
                        config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core8_Batch2);
                        break;
                    }
                    case 4: {
                        config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8_Batch4);
                        config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core8_Batch4);
                        break;
                    }
                    case 8: {
                        config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8_Batch8);
                        config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core8_Batch8);
                        break;
                    }
                }
            }
            case "log-processing": {
                switch (_batch) {
                    case 2: {
                        config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8_Batch2);
                        config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core8_Batch2);
                        break;
                    }
                    case 4: {
                        config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8_Batch4);
                        config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core8_Batch4);
                        break;
                    }
                    case 8: {
                        config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8_Batch8);
                        config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core8_Batch8);
                        break;
                    }
                }
                break;
            }
            case "spike-detection": {
                switch (_batch) {
                    case 2: {
                        config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8_Batch2);
                        config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8_Batch2);
                        config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core8_Batch2);
                        break;
                    }
                    case 4: {
                        config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8_Batch4);
                        config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8_Batch4);
                        config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core8_Batch4);
                        break;
                    }
                    case 8: {
                        config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8_Batch8);
                        config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8_Batch8);
                        config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core8_Batch8);
                        break;
                    }
                }
                break;
            }
            case "voipstream": {
                switch (_batch) {
                    case 2: {
                        config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8_Batch2);
                        config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8_Batch2);
                        config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core8_Batch2);
                        break;
                    }
                    case 4: {
                        config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8_Batch4);
                        config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8_Batch4);
                        config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core8_Batch4);
                        break;
                    }
                    case 8: {
                        config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8_Batch8);
                        config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8_Batch8);
                        config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core8_Batch8);
                        break;
                    }
                }
                break;
            }
            case "traffic-monitoring": {
                switch (_batch) {
                    case 2: {
                        config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8_Batch2);
                        config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8_Batch2);
                        break;
                    }
                    case 4: {
                        config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8_Batch4);
                        config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8_Batch4);
                        break;
                    }
                    case 8: {
                        config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8_Batch8);
                        config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8_Batch8);
                        break;
                    }
                }
                break;
            }
            case "linear-road-full": {
                switch (_batch) {
                    case 2: {
                        config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8_Batch2);
                        config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8_Batch2);
                        config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8_Batch2);
                        config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core8_Batch2);
                        break;
                    }
                    case 4: {
                        config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8_Batch4);
                        config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8_Batch4);
                        config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8_Batch4);
                        config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core8_Batch4);
                        break;
                    }
                    case 8: {
                        config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8_Batch8);
                        config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8_Batch8);
                        config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8_Batch8);
                        config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core8_Batch8);
                        break;
                    }
                }
                break;
            }
        }
    }

    public void load_TunedConfiguration_8core_HP(String _application) {

        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core8_HP);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core8_HP);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core8_HP);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core8_HP);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core8_HP);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core8_HP);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core8_HP);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core8_HP);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core8_HP);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core8_HP);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core8_HP);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core8_HP);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core8_HP);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core8_HP);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core8_HP);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core8_HP);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core8_HP);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core8_HP);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core8_HP);
                break;
            }
        }
    }

    public void load_TunedConfiguration_32core_HP_Batch(String _application) {
        switch (_application) {
            case "word-count": {
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, WordCountConstants.TunedConfiguration.Splitter_core32_HP_Batch);
                config.put(WordCountConstants.Conf.COUNTER_THREADS, WordCountConstants.TunedConfiguration.Counter_core32_HP_Batch);
                config.put("topology.acker.executors", WordCountConstants.TunedConfiguration.acker_core32_HP_Batch);
                break;
            }
            case "fraud-detection": {
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, FraudDetectionConstants.TunedConfiguration.PREDICTOR_THREADS_core32_HP_Batch);
                config.put("topology.acker.executors", FraudDetectionConstants.TunedConfiguration.acker_core32_HP_Batch);
                break;
            }
            case "log-processing": {
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, LogProcessingConstants.TunedConfiguration.GEO_FINDER_THREADS_core32_HP_Batch);
                config.put("topology.acker.executors", LogProcessingConstants.TunedConfiguration.acker_core32_HP_Batch);
                break;
            }
            case "spike-detection": {
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, SpikeDetectionConstants.TunedConfiguration.MOVING_AVERAGE_THREADS_core32_HP_Batch);
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, SpikeDetectionConstants.TunedConfiguration.SPIKE_DETECTOR_THREADS_core32_HP_Batch);
                config.put("topology.acker.executors", SpikeDetectionConstants.TunedConfiguration.acker_core32_HP_Batch);
                break;
            }
            case "voipstream": {
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, VoIPSTREAMConstants.TunedConfiguration.VAR_DETECT_THREADS_core32_HP_Batch);
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, VoIPSTREAMConstants.TunedConfiguration.FOFIR_THREADS_core32_HP_Batch);
                config.put("topology.acker.executors", VoIPSTREAMConstants.TunedConfiguration.acker_core32_HP_Batch);
                break;
            }
            case "traffic-monitoring": {
                config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, TrafficMonitoringConstants.TunedConfiguration.MAP_MATCHER_THREADS_core32_HP_Batch);
                config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, TrafficMonitoringConstants.TunedConfiguration.SPEED_CALCULATOR_THREADS_core32_HP_Batch);
                break;
            }
            case "linear-road-full": {
                config.put(LinearRoadFConstants.Conf.DispatcherBoltThreads, LinearRoadFConstants.TunedConfiguration.DispatcherBoltThreads_core32_HP_Batch);
                config.put(LinearRoadFConstants.Conf.AverageSpeedThreads, LinearRoadFConstants.TunedConfiguration.AverageSpeedThreads_core32_HP_Batch);
                config.put(LinearRoadFConstants.Conf.LatestAverageVelocityThreads, LinearRoadFConstants.TunedConfiguration.LatestAverageVelocityThreads_core32_HP_Batch);
                config.put("topology.acker.executors", LinearRoadFConstants.TunedConfiguration.acker_core32_HP_Batch);
                break;
            }
        }
    }
}
