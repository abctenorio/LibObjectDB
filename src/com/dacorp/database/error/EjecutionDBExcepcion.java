package com.dacorp.database.error;

/**
 * 
 * @author Ronald Coarite Mamani
 * @version 1.0, Compilado en Netbeans 6.9
 */
public class EjecutionDBExcepcion extends Exception
{
    public EjecutionDBExcepcion(String msg)
    {
        super(msg);
    }
    public EjecutionDBExcepcion(Throwable throwable)
    {
        super(throwable);
    }
    
    public EjecutionDBExcepcion(String msg, Throwable throwable)
    {
        super(msg, throwable);
    }
}
