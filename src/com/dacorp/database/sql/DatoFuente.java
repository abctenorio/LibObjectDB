package com.dacorp.database.sql;

import com.dacorp.database.ConectorDB;
import com.dacorp.database.Configuration;
import com.dacorp.database.annot.Table;
import com.dacorp.database.annot.enu.Schema;
import com.dacorp.database.error.EjecutionDBExcepcion;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author  Ronald Coarite Mamani
 * @version 1.0, Developed with Netbeans 6.9
 */
public class DatoFuente
{
    protected Connection connection;
    protected Statement sentencia;
    protected Configuration configuracion;
    protected String schema;

    public DatoFuente(Configuration configuracion)
    {
        this.configuracion = configuracion;
        schema = null;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public void connect() throws EjecutionDBExcepcion
    {
        if(configuracion == null)
            throw new EjecutionDBExcepcion("El Objeto Configuration es nulo.");
        try
        {
            if(configuracion.getRuta()== null)
                throw new EjecutionDBExcepcion("La ruta de la base de datos en la configuracion es nula.");
            Class.forName(configuracion.getDriver());
            connection = DriverManager.getConnection(configuracion.getRuta(), configuracion.getUsuario(), configuracion.getClave());
            try
            {
                // Agregando soporte de cursor a 
                ((org.postgresql.PGConnection)connection).addDataType("geometry",Class.forName("org.postgis.PGgeometry"));
                ((org.postgresql.PGConnection)connection).addDataType("box3d",Class.forName("org.postgis.PGbox3d"));
            }
            catch(ClassNotFoundException cne)
            {
                throw new EjecutionDBExcepcion("No tiene agregado la libreria postgis-jdbc");
            }
            
            // Tipos de cursor:
            // select
                // ResultSet.TYPE_FORWARD_ONLY  [next()]
                // ResultSet.TYPE_SCROLL_SENSITIVE [next(),last(),first(),absolute(int),relative(int),beforeFirst()]
                // ResultSet.TYPE_SCROLL_INSENSITIVE [(lo mismo pero no es posible ver la hoja de resultados)]
            // update
                // ResultSet.CONCUR_UPDATABLE [Los resultados pueden ser acutualizados]
                // ResultSet.CONCUR_READ_ONLY [Los resultados sol de solo lectura]
            sentencia = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
        }
        catch(ClassNotFoundException cnfe)
        {
            throw new EjecutionDBExcepcion("No se puede encontrar el driver. "+configuracion.getDriver());
        }
        catch(SQLException sqle)
        {
            throw new EjecutionDBExcepcion("Error al realizar la conecion. ",sqle);
        }
        catch(NullPointerException npe)
        {
            throw new EjecutionDBExcepcion("No se puede encontrar driver. "+configuracion.getDriver());
        }
    }

    /**
     * Executes any SQL statemnt
     * @param statement
     * @throws EjecucionDBExcepcion
     */
    public boolean executeSQL(String sql) throws EjecutionDBExcepcion
    {
        try
        {
            return sentencia.execute(sql);
        }
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x);
        }
    }

    /**
     *
     * @param <T>
     * @param query
     * @param handler
     * @return
     * @throws EjecucionDBExcepcion
     */
    public <T> T select(String query, Result<T> handler) throws EjecutionDBExcepcion, SQLException
    {
        ResultSet resultSet = null;
        T t = null;
        try
        {
            resultSet = sentencia.executeQuery(query);
            t = handler.handleResults(resultSet);
        }
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x);
        }
        finally
        {
            resultSet.close();
        }
        return t;
    }

    public <T> void select(String complexQuery, Object provider, final ResultHandler<T> handler) throws EjecutionDBExcepcion, SQLException
    {
        this.select(complexQuery, provider, null,handler);
    }
    /**
     *
     * @param <T>
     * @param complexQuery
     * @param provider
     * @param requiredType
     * @param handler
     * @throws SQLException
     */
    public <T> void select(String complexQuery, Object provider,
            final Class<T> requiredType, final ResultHandler<T> handler) throws EjecutionDBExcepcion, SQLException
    {
        this.select(complexQuery, provider,requiredType, handler);
    }

    public void close() throws EjecutionDBExcepcion
    {
        try 
        {
            connection.close();
            sentencia.close();
        }
        catch (SQLException ex)
        {
            throw new EjecutionDBExcepcion("Error al cerrar la Base de datos", ex);
        }
    }
    
    public void deleteTable(Class table) throws EjecutionDBExcepcion
    {
        try
        {
            sentencia.execute("DROP TABLE " +getPrefix(table)+ConectorDB.getTableName(table));
        }
        catch (SQLException x)
        {
            throw  new EjecutionDBExcepcion("Error al eliminar la tabla ["+getPrefix(table)+table+"]",x);
        }
    }
    
    public String getPrefix(Class table) throws EjecutionDBExcepcion
    {
        Table anotacion_tabla = (Table) table.getAnnotation(Table.class);
        if(anotacion_tabla == null)
            throw new EjecutionDBExcepcion("El objeto o clase ["+table.getName()+"] no esta definido como @Tabla");
        if(anotacion_tabla.schema()== Schema.PUBLIC)
            return "";
        else
        {
            if(schema == null)
                throw new EjecutionDBExcepcion("No se ha especificado ni un esquema para la tabla ["+table.getName()+"] vea setSchema(...)");
            return schema+'.';
        }
    }
}