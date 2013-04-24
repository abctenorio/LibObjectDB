package com.dacorp.database.sql.cond;

/**
 * 
 * @author Coarite Mamani Ronald
 * @version 1.0, Compilado con Netbeans 6.9
 */
public class IqualTo extends CompareTo
{
    /** Default constructor */
    public IqualTo(String attribute, Object value)
    {
        super("=", attribute, value);
    }
}