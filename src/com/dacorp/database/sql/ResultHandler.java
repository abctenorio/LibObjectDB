package com.dacorp.database.sql;

/**
 * @author Coarite Mamani Ronald
 * @version 1.0, Compilado con Netbeans 6.9
 */
public interface ResultHandler<S>
{
    public void handleObject(S resultado) throws Exception;
}