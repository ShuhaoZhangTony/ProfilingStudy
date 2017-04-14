package flink.applications.bolt.batch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import flink.applications.bolt.base.AbstractBolt;
import flink.applications.constants.TrafficMonitoringConstants.Conf;
import flink.applications.constants.TrafficMonitoringConstants.Field;
import flink.applications.model.gis.GPSRecord;
import flink.applications.model.gis.RoadGridList;
import flink.applications.util.stream.StreamValues;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn
 */
public class BatchMapMatchingBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(BatchMapMatchingBolt.class);

    private RoadGridList sectors;
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;

    @Override
    public void initialize() {
        String shapeFile = config.getString(Conf.MAP_MATCHER_SHAPEFILE);

        latMin = config.getDouble(Conf.MAP_MATCHER_LAT_MIN);
        latMax = config.getDouble(Conf.MAP_MATCHER_LAT_MAX);
        lonMin = config.getDouble(Conf.MAP_MATCHER_LON_MIN);
        lonMax = config.getDouble(Conf.MAP_MATCHER_LON_MAX);

        try {
            sectors = new RoadGridList(config, shapeFile);
        } catch (SQLException | IOException ex) {
            LOG.error("Error while loading shape file", ex);
            throw new RuntimeException("Error while loading shape file");
        }
    }

    @Override
    public void execute(Tuple input) {
        try {

        /*        spout.setFields(new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE));
                */
//input..
            String[] batch_cardID = (String[]) input.getValue(0);
            int batch = batch_cardID.length;
            DateTime[] batch_date = (DateTime[]) input.getValue(1);
            boolean[] batch_occ = (boolean[]) input.getValue(2);
            int[] batch_speed = (int[]) input.getValue(3);
            int[] batch_bearing = (int[]) input.getValue(4);
            double[] batch_lat = (double[]) input.getValue(5);
            double[] batch_lon = (double[]) input.getValue(6);

//output..
            LinkedList<String> batch_cardID_out = new LinkedList<String>();
            LinkedList<DateTime> batch_date_out = new LinkedList<DateTime>();
            LinkedList<Boolean> batch_occ_out = new LinkedList<Boolean>();
            LinkedList<Integer> batch_speed_out = new LinkedList<Integer>();
            LinkedList<Integer> batch_bearing_out = new LinkedList<Integer>();
            LinkedList<Double> batch_lat_out = new LinkedList<Double>();
            LinkedList<Double> batch_lon_out = new LinkedList<Double>();

            LinkedList<Integer> batch_roadID = new LinkedList<Integer>();


            for (int i = 0; i < batch; i++) {
                int speed = batch_speed[i];
                int bearing = batch_bearing[i];
                double latitude = batch_lat[i];
                double longitude = batch_lon[i];
//            if (speed <= 0) {
//                System.out.print("");
//                return;
//            }
//            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin){
//                System.out.print("");
//                return;
//            }
                GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);

                int roadID = sectors.fetchRoadID(record);

                if (roadID != -1) {

                    //     List<Object> values = input.getValues();
                    //    values.add(roadID);

                    //       collector.emit(input, values);
                    batch_cardID_out.add(batch_cardID[i]);
                    batch_date_out.add(batch_date[i]);
                    batch_occ_out.add(batch_occ[i]);
                    batch_speed_out.add(batch_speed[i]);
                    batch_bearing_out.add(batch_bearing[i]);
                    batch_lat_out.add(batch_lat[i]);
                    batch_lon_out.add(batch_lon[i]);
                    batch_roadID.add(roadID);
                }
            }
            if (batch_bearing_out.size() >= 1)
                collector.emit(input, new StreamValues(batch_cardID_out, batch_date_out, batch_occ_out, batch_speed_out, batch_bearing_out, batch_lat_out, batch_lon_out, batch_roadID));

            collector.ack(input);
        } catch (SQLException ex) {
            LOG.error("Unable to fetch road ID", ex);
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE, Field.ROAD_ID);
    }
}
