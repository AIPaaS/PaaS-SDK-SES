package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;

/**
 * 搜索引擎中的位置基础对象，含有经纬度
 * 
 * @author douxiaofeng
 *
 */
public class GeoLocation implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 2131664114544142612L;

    public GeoLocation() {

    }

    public GeoLocation(String field, double lat, double lon, double distance) {
        this.geoField = field;
        this.location = new Location(lat, lon);
        this.distance = distance;
    }

    private Location location;

    /**
     * 代表位置的ES中的字段
     */
    private String geoField;
    /**
     * 距离某个点的距离
     */
    private double distance;

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    public String getGeoField() {
        return geoField;
    }

    public void setGeoField(String geoField) {
        this.geoField = geoField;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public class Location implements Serializable {
        /**
         * 
         */
        private static final long serialVersionUID = -6417238185814841607L;

        public Location(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }

        /**
         * 位置的纬度
         */
        private double lat;
        /**
         * 位置的经度
         */
        private double lon;

        public double getLat() {
            return lat;
        }

        public void setLat(double lat) {
            this.lat = lat;
        }

        public double getLon() {
            return lon;
        }

        public void setLon(double lon) {
            this.lon = lon;
        }

    }
}
