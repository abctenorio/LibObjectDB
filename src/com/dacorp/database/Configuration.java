package com.dacorp.database;

import com.dacorp.database.annot.Table;
import com.dacorp.database.annot.enu.Schema;

/**
 * HERCOM Software
 * @author "Ronald Coarite Mamani"
 */
public class Configuration
{
    private String driver;
    private String usuario;
    private String clave;
    private String ruta;
    
    public static final String MYSQL_DRIVER="com.mysql.jdbc.Driver";
    public static final String POSTGRESS_DRIVER="org.postgresql.Driver";

    public Configuration(String driver, String usuario, String clave, String ruta)
    {
        this.driver = driver;
        this.usuario = usuario;
        this.clave = clave;
        this.ruta = ruta;
    }

    public Configuration() {
    }

    public String getClave() {
        return clave;
    }

    public String getDriver() {
        return driver;
    }

    public String getRuta() {
        return ruta;
    }

    public String getUsuario() {
        return usuario;
    }

    public void setClave(String clave) {
        this.clave = clave;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public void setRuta(String ruta) {
        this.ruta = ruta;
    }

    public void setUsuario(String usuario) {
        this.usuario = usuario;
    }
}
