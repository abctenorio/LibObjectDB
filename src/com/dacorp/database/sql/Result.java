package com.dacorp.database.sql;

import com.dacorp.database.error.EjecutionDBExcepcion;
import java.sql.ResultSet;

/*
 * @author  Coarite Mamani Ronald
 * @version 1.0, Developed with Netbeans 6.5
 */
public interface Result <T>
{
    public T handleResults(ResultSet resultSet)throws EjecutionDBExcepcion;
}