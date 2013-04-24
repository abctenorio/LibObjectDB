package com.dacorp.database.sql.cond;

/**
 * Libreria basica para la manipulacion de las bases de datos orientado a objetos.
 * @author  Ronadl Coarite Mamani
 * @version 1.0, Compilado con Netbeans 6.9
 */
public class Condition
{
    private String condicion;

    /**
     * La Condicion que sera incluida en el WHERE de la creacion 
     * del sql de la consulta
     * @param condicion 
     */
    public Condition(String condicion)
    {
        this.condicion = condicion;
    }


    /**
     *
     * @return 
     */
    public String parse()
    {
        return condicion;
    }
    
    public static final Condition ALL = new Condition("");
}