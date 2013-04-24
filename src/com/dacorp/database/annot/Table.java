package com.dacorp.database.annot;

import com.dacorp.database.annot.enu.Schema;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * use this interface to define an object to be used as an entity
 * @author Ronald Coarite Mamani
 * @version 1.0, Developed with Netbeans 6.9
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table
{
    public Schema schema();
}