package com.dacorp.database.annot;

import com.dacorp.database.annot.enu.Relation;
import com.dacorp.database.data.Binari;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Esta anotacion representa a una columna de una tabla
 * @author  Coarite Mamani Nemecio Ronald
 * @version 1.0, Compilado con Netbeans 6.9
 */
@Documented
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column
{
    public boolean primary_key() default false;
    public boolean auto_incrementing() default false;
    public boolean nulo() default false;
    public String type() default Binari.TYPE_STREAM;
    public Class foreing_key() default Object.class;
    public Relation relation() default Relation.SEVERAL;
}