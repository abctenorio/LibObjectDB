package com.dacorp.database.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * HERCOM Software
 * Domotec - Sistema Domotico
 * @author Ronald Coarite Mamani
 */
public class Binari
{
    public static final String TYPE_XML="text/xml";
    public static final String TYPE_PHOTO="image/png";
    public static final String TYPE_STREAM="aplication/octect";
    public static final String TYPE_JSON="aplication/json";
    
    private InputStream input;

    public Binari(File file) throws Exception
    {
        input = new FileInputStream(file);
    }

    public Binari(InputStream input)
    {
        this.input = input;
    }

    public InputStream getInputStrema()
    {
        return input;
    }

    public void escribir(OutputStream output) throws Exception
    {
        byte fraccion[] = new byte[1024];// un mega
        int leidos = input.read(fraccion);
        while(leidos!=-1)
        {
            output.write(fraccion, 0, leidos);
            output.flush();
            leidos = input.read(fraccion);
        }
        output.flush();
        input.close();
        output.close();
    }

    public void cerrar()
    {
        try
        {
            input.close();
        }
        catch (Exception e)
        {
        }
    }
}