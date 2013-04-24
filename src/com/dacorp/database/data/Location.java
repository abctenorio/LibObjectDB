/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dacorp.database.data;

/**
 *
 * @author Ronald Coarite Mamani
 */
public class Location
{
    private double longitude;
    private double latitude;
    private double altitude;

    public Location(double longitude, double latitude,double altitude)
    {
        this.longitude = longitude;
        this.latitude = latitude;
        this.altitude = altitude;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(longitude);
        builder.append(' ');
        builder.append(latitude);
        builder.append(' ');
        builder.append(altitude);
        return builder.toString();
    }

    public Location()
    {
    }

    public double getAltitude() {
        return altitude;
    }

    public void setAltitude(double altitude) {
        this.altitude = altitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    } 
}
