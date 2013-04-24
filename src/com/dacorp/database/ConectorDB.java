package com.dacorp.database;

import com.dacorp.database.annot.Column;
import com.dacorp.database.annot.Table;
import com.dacorp.database.error.EjecutionDBExcepcion;
import com.dacorp.database.sql.DatoFuente;
import com.dacorp.database.sql.ResultHandler;
import com.dacorp.database.sql.ResultReader;
import com.dacorp.database.sql.Result;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import com.dacorp.database.sql.cond.Condition;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import javax.naming.directory.NoSuchAttributeException;

/**
 * @author Coarite Mamani Ronald
 * @version 1.0, Developed with Netbeans 6.9
 */
public class ConectorDB extends DatoFuente
{
    /**
     * Crea una conexion con la configuracion
     * @param configuracion Configuracion de acceso a la base de datos
     */
    public ConectorDB(Configuration configuracion)
    {
        super(configuracion);
    }
    
    public boolean createSchema(String schema) throws EjecutionDBExcepcion
    {
        //CREATE SCHEMA cliente_2;
        return executeSQL("CREATE SCHEMA "+schema);
    }
    
    public List<String> getSchemas()
    {
        final List<String> list = new ArrayList<String>(2);
        try
        {
            StringBuilder builder = new StringBuilder();
            builder.append("select schema_name from information_schema.schemata");
            builder.append(" where schema_name <> 'information_schema' and schema_name !~ E'^pg_'");
            select(builder.toString(),new Result<Void>()
            {
                @Override
                public Void handleResults(ResultSet resultSet) throws EjecutionDBExcepcion
                {
                    try
                    {
                        while (resultSet.next())
                            list.add(resultSet.getString(1));
                    }
                    catch (Exception e)
                    {
                    }
                    return null;
                }
            });
        }
        catch (Exception e)
        {
        }
        return list;
    }

    public void setConfiguration(Configuration configuracion)
    {
        this.configuracion = configuracion;
    }

    /**
     * Elimina todos los registros de la tabla que cumplan la condicion
     * @param entity Un objeto especifico o una tabla
     * @param condicion
     * @return Retorna el numero de registros que han sido modificados
     */
    public int delete(Object tabla, Condition condicion) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. conectar()");
        String nombre_tabla = getTableName(tabla);
        String sql;
        
        if(Condition.ALL == condicion)
            sql = "DELETE FROM " + getPrefix(getClase(tabla)) + nombre_tabla;
        else
            sql = "DELETE FROM " + getPrefix(getClase(tabla)) + nombre_tabla +" WHERE "+condicion.parse();
        try
        {
            return sentencia.executeUpdate(sql);
        }
        catch (SQLException ex)
        {
            throw new EjecutionDBExcepcion("Error al borra la tabla ["+getPrefix(getClase(tabla))+tabla+"]",ex);
        }
    }

    private static Class getClase(Object tabla) throws EjecutionDBExcepcion
    {
        Class clase = (tabla instanceof Class) ? (Class) tabla : tabla.getClass();
        if(clase.isInterface())
            throw new EjecutionDBExcepcion("La interface ["+clase.getName()+"] debe ser una clase");
        Table anotacion_tabla = (Table) clase.getAnnotation(Table.class);
        if(anotacion_tabla == null)
            throw new EjecutionDBExcepcion("El objeto o clase ["+clase.getName()+"] no esta definido como @Tabla");
        return clase;
    }

    /**
     * Obtiene el nombre de la tabla de un objeto que puede ser una clase o una entidad
     * @param tabla El objeto tabla o entidad
     * @return El nombre de la tabla
     * @throws EjecucionDBExcepcion Si el objeto no esta definido como @Tabla
     */
    public static String getTableName(Object tabla) throws EjecutionDBExcepcion
    {
        try
        {
            if(tabla instanceof String)
                return String.valueOf(tabla);
            Class clase = getClase(tabla);
            return clase.getSimpleName();
        }
        catch (Exception x)
        {
            throw new EjecutionDBExcepcion("Error al obtener el nombre de la tabla ", x);
        }
    }

    /**
     * Funcion que obtiene el nombre de la columna declarada como llave primaria
     * si lo posee.
     * @param tabla El objeto o clase declarada como Tabla
     * @return El nombre de la llave primara de la tabla
     * @throws EjecucionDBExcepcion En caso de producirce un error
     * @see Tabla
     * @see Columna
     */
    public static String getNamePK(Object tabla) throws EjecutionDBExcepcion
    {
        Class clase = getClase(tabla);
        List<Field> atributos = new ArrayList<Field>(10);
        ResultReader.recolectarFields(atributos,clase);
        for (Field field : atributos)
        {
            Column columna = field.getAnnotation(Column.class);
            if(columna.primary_key())
            {
                return field.getName();
            }
        }
        return null;
        //throw new EjecutionDBExcepcion("La tabla [ "+getTableName(tabla)+" ] no contiene una columna con una llave primara");
    }

    private static HashMap<String,Class> tables = new HashMap<String, Class>(7);
    /**
     * 
     * @param entity Object must be an Tabla interface.
     */
    public void createORemplaceTable(Object entity) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        try
        {
            StringBuilder create = new StringBuilder("CREATE TABLE "); //IF NOT EXISTS  pg v9.0
            String nombre_tabla = getTableName(entity);
            Class clase = getClase(entity);
            create.append(getPrefix(getClase(entity)));
            create.append(nombre_tabla);
            create.append("(");
            List<Field> atributos = ResultReader.recolectarFields(clase);
            int nro_autoincremtables = 0;
            int nro_llaves = 0;
            String nombre_primari = null;
            // Buscamos todas la columnas para escribir la cosulta
            for (Field field : atributos)
            {
                Column columna = field.getAnnotation(Column.class);
                if(columna.auto_incrementing())
                {
                    nro_autoincremtables++;
                    if(nro_autoincremtables >1)
                        throw new EjecutionDBExcepcion("No se deben definir dos auto encrementables Columna ["+field.getName()+"]");
                }
                if(columna.primary_key())
                {
                    nro_llaves++;
                    if(nro_llaves >1)
                        throw new EjecutionDBExcepcion("No se deben definir dos llaves primarias Columna ["+field.getName()+"]");
                    nombre_primari = field.getName();
                }
                Class type = field.getType();
                ResultReader.addSQLTipo(create, columna,type,field.getName());
            }
            int lio = create.lastIndexOf(",");
            if (lio > 0)
            {
                create = create.replace(lio, lio + 1, ")");
                // Verificamos si la tabla tiene una clase padre
                Class supercs = clase.getSuperclass();
                //boolean exist_superclasss = false;
                if(!supercs.isAssignableFrom(Object.class))
                {
                    Class super_clase = getClase(supercs);
                    //CREATE TABLE capitales (departamento  char(2)) INHERITS (ciudades);
                    create.append(" INHERITS (");
                    create.append(getPrefix(super_clase));
                    create.append(super_clase.getSimpleName());
                    create.append(")");
                    //exist_superclasss = true;
                }
                
                //deleteTabla(nombre_tabla);
                //System.out.println("Creando tabla: "+create.toString());
                sentencia.execute(create.toString());
                // Verificamos si existe una llave primaria en la tabla
                // para crearla
                
                //if(nombre_primari!=null&&!existsTable(clase))
                if(nombre_primari!=null)
                {
                    //System.out.println("Agregando llave primaria: "+nombre_primari);
                    // ALTER TABLE distributors ADD PRIMARY KEY (dist_id);
                    try
                    {
                        String cs = "ALTER TABLE "+getPrefix(getClase(entity))+nombre_tabla+" ADD PRIMARY KEY ("+nombre_primari+");";
                        sentencia.execute(cs);
                    }
                    catch (Exception e){}
                }
            }
            else
            {
                throw new EjecutionDBExcepcion("Noce puede crear la tabla ["+nombre_tabla+"] porque no tiene ningun atributo");
            }
            tables.put(clase.getSimpleName(), clase);
        } 
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x);
        }
    }
    

    /**
     * Actualiza todos los registros de la tabla, pero solo afecta la columanas
     * que pertenecen al vector.
     * @param <U>
     * @param entity La tabla
     * @param columns Las columnas que se desa afectar
     * @param condicion Condicion para la actualizacion
     */
    public <U> int update(U entity,List<String> columnas_afectar, Condition condicion) throws EjecutionDBExcepcion
    {
        String vector[] = new String[columnas_afectar.size()];
        return update(entity, columnas_afectar.toArray(vector), condicion);
    }

    /**
     * Actualiza todos los registros de la tabla, pero solo afecta la columanas
     * que pertenecen al vector.
     * @param <U>
     * @param entity La tabla
     * @param columns Las columnas que se desa afectar
     * @param condicion Condicion para la actualizacion
     */
    public <U> int update(U entity, String columnas_afectar[], Condition condicion) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. conectar()");
        try
        {
            //need to evaluate this as a valid entity object
            Class clase = getClase(entity);
            final String nombre_tabla = getTableName(entity);
            StringBuilder builder = new StringBuilder("UPDATE "+getPrefix(getClase(entity))+nombre_tabla+" SET ");

            //System.out.println("Numero de columnas: "+columnas_afectar.length);
            // UPDATE tabla SET columna1=valor1, columna2=valor2,... WHERE condición;
            for (int indice =0;indice<columnas_afectar.length;indice++)
            {
                builder.append(columnas_afectar[indice]);
                builder.append("=?");
                if(indice+1<columnas_afectar.length)
                    builder.append(",");
            }
            if(condicion != Condition.ALL&&condicion.parse().length()>0)
            {
                builder.append(" WHERE ");
                builder.append(condicion.parse());
            }
            String sql = builder.toString();
            //System.out.println("SQL update: "+sql);
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            for (int posicion = 0;posicion<columnas_afectar.length;posicion++)
            {
                Field atributo;
                try
                {
                    atributo = buscarDeclaredField(columnas_afectar[posicion], clase,clase);
                }
                catch (Exception ex)
                {
                    throw new EjecutionDBExcepcion(ex.getMessage());
                }
                // No afectar cuando no es una anotacion o es la llave primaria
                Column c = atributo.getAnnotation(Column.class);
                if (c == null || c.auto_incrementing())
                    continue;
                //String cname = c.value().trim();
                //cname = cname.length() > 0 ? cname : atributo.getName();
                try
                {
                    Method m = clase.getMethod(ResultReader.getGetterName(atributo));
                    Object o = m.invoke(entity);
                    ResultReader.addValor(preparedStatement, posicion+1, o);
                }
                catch (Exception ex)
                {
                    throw new EjecutionDBExcepcion("No se encuentra el metodo ["+ResultReader.getSetterName(atributo)+"()] en la clase["+clase.getSimpleName()+"]", ex);
                }
            }
            return preparedStatement.executeUpdate();
        }
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x);
        }
    }
    
    public <T> int updateColumn(Class clase,String name_column,T value,Condition condition) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. conectar()");
        try
        {
            final String nombre_tabla = getTableName(clase);

            StringBuilder builder = new StringBuilder("UPDATE ");
            builder.append(getPrefix(clase));
            builder.append(nombre_tabla);
            builder.append(" SET ");

            String[] columns = getColumnas(clase);
            boolean exist_table = false;
            for(String colum:columns)
            {
                if(colum.equals(name_column))
                {
                    exist_table = true;
                    break;
                }
            }
            if (!exist_table)
                throw new EjecutionDBExcepcion("No existe la columna ["+name_column+"] en la tabla ["+nombre_tabla+"]");
            builder.append(name_column);
            builder.append("=?");
            if(condition != Condition.ALL&&condition.parse().length()>0)
            {
                builder.append(" WHERE ");
                builder.append(condition.parse());
            }
            String sql = builder.toString();
            //System.out.println(getClass().getName()+":SQL update column: "+sql);
        
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            Field atributo;
            try
            {
                atributo = buscarDeclaredField(name_column, clase,clase);
            }
            catch (Exception ex)
            {
                throw new EjecutionDBExcepcion(ex.getMessage());
            }
            // No afectar cuando no es una anotacion o es la llave primaria
            Column c = atributo.getAnnotation(Column.class);
            if (c == null || c.auto_incrementing())
                return -1;
            //String cname = c.value().trim();
            //cname = cname.length() > 0 ? cname : atributo.getName();
            //System.out.println(getClass().getName()+":"+value);
            ResultReader.addValor(preparedStatement,1, value);
            return preparedStatement.executeUpdate();
        }
        catch(EjecutionDBExcepcion es)
        {
            throw es;
        }
        catch (SQLException e)
        {
            throw new EjecutionDBExcepcion(ConectorDB.class.getName()+" : Error interno al preparar la actualizacion: "+e);
        }
    }
    
    public <T> T selectColumn(final Class clase,String name_column,final Condition condition) throws EjecutionDBExcepcion, NoSuchAttributeException
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        final Field field = buscarDeclaredField(name_column, clase, clase);
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        builder.append(field.getName());
        builder.append(" FROM ");
        builder.append(getPrefix(clase));
        builder.append(clase.getSimpleName());
        if(condition!=Condition.ALL&&condition.parse().length()>0)
        {
            builder.append(" WHERE ");
            builder.append(condition.parse());
        }
        builder.append(" LIMIT 1");
        
        Object object = select(builder.toString(),new Result<Object>()
        {
            @Override
            public Object handleResults(ResultSet resultSet) throws EjecutionDBExcepcion
            {
                try
                {
                    if(resultSet.next())
                    {
                        Object object = ResultReader.leerValorColumna(resultSet,field.getType(), 1);
                        return object;
                    }
                    else
                        throw new EjecutionDBExcepcion("No existen resultados con la condicion ["+condition.parse()+"] en la tabla ["+getPrefix(clase)+clase.getSimpleName()+"]");
                }
                catch (SQLException ex)
                {
                    throw new EjecutionDBExcepcion(ex);
                }
            }
        });
        return (T) ResultReader.comvertType(object, field);
    }
    
    public static Field buscarDeclaredField(String column,Class clase,Class clase_fuente) throws NoSuchAttributeException,EjecutionDBExcepcion
    {
        String nombre = clase.getSimpleName();
        boolean esObjeto = nombre.equals(Object.class.getSimpleName());
        if(esObjeto)
            throw new NoSuchAttributeException("No existe la columna ["+column+"] en la tabla ["+clase_fuente.getSimpleName()+"]");
        try 
        {
            Field field = clase.getDeclaredField(column);
            Column c = field.getAnnotation(Column.class);
            if (c == null)
                throw new EjecutionDBExcepcion("La columna  ["+column+"] en la tabla ["+clase_fuente.getSimpleName()+"] no esta declarada con @Column");
            return field;
        }
        catch (NoSuchFieldException ex)
        {
            return buscarDeclaredField(column,clase.getSuperclass(),clase_fuente);
        }
    }

    /**
     * Updates this object replacing all the values in it.
     * WARNING, this can't update a column of type SERIAL
     * @param <U>
     * @param tabla
     * @param condicion
     * @return the number of rows affected
     */
    public <U> int update(U tabla, Condition condicion) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        return update(tabla,getColumnas(tabla), condicion);
    }

    /**
     * Obtine todas las columnas inclidas de las superclases
     * @param tabla
     * @return
     * @throws EjecucionDBExcepcion 
     */
    protected String[] getColumnas(Object tabla) throws EjecutionDBExcepcion
    {
        Class clase = getClase(tabla);
        ArrayList<String> columnas = new ArrayList<String>(5);
        recolectarColumnas(columnas, clase);
        columnas.trimToSize();
        String array[] = new String[columnas.size()];
        array = columnas.toArray(array);
        return array;
    }
    
    /**
     * Este metodo recolecta todas las columnas desde la clase root hasta la clase Object en busca
     * de todas las definiciones de las columnas
     * @param columnas Una lista de columnas acumulada
     * @param root La Clase con la que inicia la busqueda
     */
    protected void recolectarColumnas(List<String> columnas,Class root)
    {
        String nombre = root.getSimpleName();
        if(nombre.equals(Object.class.getSimpleName()))
            return;
        for (Field field : root.getDeclaredFields())
        {
            Column c = field.getAnnotation(Column.class);
            if (c == null ||c.auto_incrementing())
                continue;
            String nombre_columna = field.getName().trim();//c.value().trim();
            nombre_columna = nombre_columna.length() > 0 ? nombre_columna : field.getName();
            columnas.add(nombre_columna);
        }
        recolectarColumnas(columnas,root.getSuperclass());
    }

    /**
     *
     * @param <U>
     * @param entity
     * @return the row ID of the newly inserted row, or -1 if an error occurred
     * @throws EjecucionDBExcepcion
     */
    public int insert(Object entity) throws EjecutionDBExcepcion
    {
        if(entity == null)
            throw new EjecutionDBExcepcion("El objeto a insertar es nulo");
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        try
        {
            if(entity instanceof Class)
                throw new EjecutionDBExcepcion("El objeto a inserta debe ser una instancia de un objeto con @Tabla");
            
            Class clase = entity.getClass();
            String nombre_tabla = getTableName(entity);
            // ("insert into usuarios(nombre, identificacion) 
            StringBuilder builder = new StringBuilder("INSERT INTO "+getPrefix(getClase(entity))+nombre_tabla +" (");
            String columnas[] = getColumnas(entity);
            for(String columna:columnas)
            {
                builder.append(columna);
                builder.append(',');
            }
            builder.deleteCharAt(builder.length()-1);
            builder.append(')');
            
            // INSERT INTO "nombre_tabla" ("columna1", "columna2", ...) VALUES ("valor1", "valor2", ...)
            
            builder.append(" VALUES (");
            for(String columna:columnas)
            {
                builder.append("?");
                builder.append(',');
            }
            builder.deleteCharAt(builder.length()-1);
            builder.append(')');
            
            String pk_table = getNamePK(clase);
            if(pk_table != null)
            {
                builder.append(" returning ");
                builder.append(pk_table);
            }
            
            String sql = builder.toString();
            //System.out.println(getClass().getName()+":SQL insert: "+sql);
            
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            int cont = 1;
            ArrayList<Field> atributos = new ArrayList<Field>(20);
            ResultReader.recolectarFields(atributos, clase);
            for (Field field : atributos)
            {
                if(cont>columnas.length)
                    break;
                Column c = field.getAnnotation(Column.class);
                if(c.auto_incrementing())
                    continue;
                try
                {
                    Method m = clase.getMethod(ResultReader.getGetterName(field));
                    Object o = m.invoke(entity);
                    ResultReader.addValor(preparedStatement,cont, o);
                    cont++;
                }
                catch(SecurityException se)
                {
                    throw new EjecutionDBExcepcion("No se puede acceder al metodo ["+ResultReader.getGetterName(field)+"()] en la clase["+nombre_tabla+"]", se);
                }
                catch (NoSuchMethodException nsme)
                {
                    throw new EjecutionDBExcepcion("No se encuentra el metodo ["+ResultReader.getGetterName(field)+"()] en la clase["+nombre_tabla+"]", nsme);
                }
                catch (Exception ex)
                {
                    throw new EjecutionDBExcepcion("Error de insersion: "+ex.getMessage(), ex);
                }
            }
            if(pk_table == null)
            {
                return preparedStatement.executeUpdate();
            }
            else
            {
                ResultSet resultSet = preparedStatement.executeQuery();
                // Otenemos el id con el que se inserto el AUTOINCREMENTABLE
                if(resultSet.next())
                    return resultSet.getInt(pk_table);
            }
            return -1;
        } 
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x.getMessage(), x);
        }
    }

    /**
     *
     * @param <T>
     * @param query
     * @param handler
     * @return
     * @throws DataSourceException
     */
    @Override
    public <T> T select(String query, Result<T> handler) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        T t = null;
        try
        {
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,ResultSet.CONCUR_READ_ONLY);
            ResultSet c = statement.executeQuery(query);
            t = handler.handleResults(c);
            c.close();
            statement.close();
        }
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x);
        }
        return t;
    }

    public <T> void select(String sql,final Class<T> clase, final ResultHandler<T> handler) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        select(sql, new Result<T>()
        {
            @Override
            public T handleResults(ResultSet rs) throws EjecutionDBExcepcion
            {
                try 
                {
                    while (rs.next())
                    {
                        T obj = ResultReader.doMap(rs, clase);
                        handler.handleObject(obj);
                    }
                }
                catch (Exception ex)
                {
                    throw new EjecutionDBExcepcion(ex);
                }
                return null;
            }
        });
    }
    
    public <T> List<T> select(String sql,final Class<T> clase) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        final List<T> res = new LinkedList<T>();
        select(sql, new Result<T>()
        {
            @Override
            public T handleResults(ResultSet rs) throws EjecutionDBExcepcion
            {
                try 
                {
                    while (rs.next())
                    {
                        T obj = ResultReader.doMap(rs, clase);
                        res.add(obj);
                    }
                }
                catch (Exception ex)
                {
                    throw new EjecutionDBExcepcion(ex);
                }
                return null;
            }
        });
        return res;
    }
    
    /**
     * Selects all the records from the entity provided
     * @param <U>
     * @param clazz
     * @return a collection of all objects retrieved from entity
     * @throws EjecucionDBExcepcion
     */
    public <U> List<U> select(Class<U> clazz) throws EjecutionDBExcepcion
    {
        return this.select(clazz, Condition.ALL);
    }

    /**
     * Select all the results from an entity using a filter
     * @param <U>
     * @param clazz
     * @param filter
     * @return a collection of all results that match the given filter
     * @throws EjecucionDBExcepcion
     */
    public <U> List<U> select(Class<U> clazz, Condition filter) throws EjecutionDBExcepcion
    {
        return this.select(clazz, filter, null, null, null);
    }

    public <U> List<U> select(Class<U> clazz, Condition filter, String orderBy) throws EjecutionDBExcepcion
    {
        return this.select(clazz, filter, null, null, orderBy);
    }

    private <U> List<U> select(Class<U> clase, Condition condicion, String groupBy, String having, String orderBy) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        try
        {
            String nombre_tabla = getTableName(clase);
            //must retrieve all the columns for create it
//            String columns = "";
//            //retrieve data about the fields
//            for (Field field : clazz.getDeclaredFields()) {
//                //if the field is a XBean, then the process must be treated separately
//                Columna c = field.getAnnotation(Columna.class);
//                if (c == null) {
//                    continue;
//                }
//                String nombre_columna = c.value().trim();
//                nombre_columna = nombre_columna.length() > 0 ? nombre_columna : field.getName();
//                columns += nombre_columna + ",";
//            }
            StringBuilder builder = new StringBuilder("SELECT * FROM "+ getPrefix(clase)+nombre_tabla);
            if(condicion != Condition.ALL)
            {
                builder.append(" WHERE ");
                builder.append(condicion.parse());
            }
            if(orderBy != null)
            {
                builder.append(" ORDER BY ");
                builder.append(orderBy);
            }
            if(groupBy!=null)
            {
                builder.append(" GROUP BY ");
                builder.append(groupBy);

                if(having!=null)
                {
                    builder.append(" HAVING ");
                    builder.append(having);
                }
            }
            String sql = builder.toString();
            //System.out.println("CONSULTA = "+sql);
            ResultSet resultSet = sentencia.executeQuery(sql);
            List<U> res = new LinkedList<U>();
            while (resultSet.next())
            {
                U u = ResultReader.doMap(resultSet,clase);
                res.add(u);
            }
            
            resultSet.close();
            return res;
        } 
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x.getMessage(), x);
        }
    }

    /**
     * Select all records, but without returning a collection, but using
     * the {@link ResultHandler} to retrieve the results found.
     * @param <U>
     * @param clazz
     * @param handler
     * @throws EjecucionDBExcepcion
     */
    public <U> void select(Class<U> clazz, ResultHandler<U> handler) throws EjecutionDBExcepcion
    {
        this.select(clazz, Condition.ALL, handler, null, null, null);
    }

    /**
     * Select all records as result of the filter, but without returning a collection, but using
     * the {@link ResultHandler} to retrieve the results found.
     * @param <U>
     * @param clazz
     * @param handler
     * @throws EjecucionDBExcepcion
     */
    public <U> void select(Class<U> clazz, Condition filter, ResultHandler<U> handler) throws EjecutionDBExcepcion
    {
        this.select(clazz, filter, handler, null, null, null);
    }

    public <U> void select(Class<U> clazz, Condition filter, ResultHandler<U> handler, String orderBy) throws EjecutionDBExcepcion
    {
        this.select(clazz, filter, handler, null, null, orderBy);
    }

    public <U> void select(Class<U> clazz, Condition filter, ResultHandler<U> handler, String groupBy, String having, String orderBy) throws EjecutionDBExcepcion
    {
        try
        {
            String nombre_tabla = getTableName(clazz);
            //must retrieve all the columns for create it
//            String columns = "";
//            //retrieve data about the fields
//            for (Field field : clazz.getDeclaredFields()) {
//                //if the field is a XBean, then the process must be treated separately
//                Columna c = field.getAnnotation(Columna.class);
//                if (c == null) {
//                    continue;
//                }
//                String nombre_columna = c.value().trim();
//                nombre_columna = nombre_columna.length() > 0 ? nombre_columna : field.getName();
//                columns += nombre_columna + ",";
//            }
            String sql;
            if(filter == Condition.ALL)
                sql = "SELECT * FROM " + getPrefix(clazz)+nombre_tabla;
            else
                sql = "SELECT * FROM " + getPrefix(clazz)+nombre_tabla +" WHERE "+filter.parse();
            ResultSet resultSet = sentencia.executeQuery(sql);
//                    query(ename, columns.split(","), filter.parse(), null, groupBy, having, orderBy);
            while (resultSet.next())
            {
                U u = ResultReader.doMap(resultSet, clazz);
                handler.handleObject(u);
            }
            resultSet.close();
        }
        catch (Exception x)
        {
            throw new EjecutionDBExcepcion(x.getMessage(), x);
        }
    }

    /**
     * Returns the first result found in the netire cursor.
     * @param <U>
     * @param clazz
     * @param filter
     * @return the first result found
     * @throws EjecucionDBExcepcion
     */
    public <U> U selectFirst(Class<U> clase,Condition condicion) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        ResultSet cursor;
        try
        {
            String ename = getTableName(clase);
            //must retrieve all the columns for create it
//            String columns = "";
//            //retrieve data about the fields
//            for (Field field : clazz.getDeclaredFields())
//            {
//                //if the field is a XBean, then the process must be treated separately
//                Columna c = field.getAnnotation(Columna.class);
//                if (c == null)
//                {
//                    continue;
//                }
//                String nombre_columna = c.value().trim();
//                nombre_columna = nombre_columna.length() > 0 ? nombre_columna : field.getName();
//                columns += nombre_columna + ",";
//            }

            String sql;
            if(condicion == Condition.ALL)
                sql = "SELECT * FROM " + getPrefix(clase)+ename;
            else
                sql = "SELECT * FROM " + getPrefix(clase)+ename +" WHERE "+condicion.parse();
            
//            System.out.println("\n"+sql);
            cursor = sentencia.executeQuery(sql);
            if(cursor.first())
            {
                U u = ResultReader.doMap(cursor, clase);
                return u;
            }
            cursor.close();
            return null;
        }
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x.getMessage(), x);
        }
    }

    /**
     * Verifia si una tabla tiene registros o no
     * @param <U> El tipo de Resultado de Respuesta
     * @param tabla La tabla que se quiere comparar
     * @return true si la tabla esta vacia y false si no lo esta
     * @throws EjecucionDBExcepcion
     */
    public <U> boolean isEmply(U tabla) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        try
        {
            ResultSet resultSet = sentencia.executeQuery("SELECT * FROM " + getPrefix(getClase(tabla))+getTableName(tabla) + " LIMIT 1");
            boolean emp = !resultSet.next();
            resultSet.close();
            return emp;
        }
        catch (SQLException x)
        {
            throw new EjecutionDBExcepcion(x.getMessage(), x);
        }
    }

    public <U> int getCountRecords(U Tabla,Condition condicion) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        String sql = "SELECT count(*) FROM "+getPrefix(getClase(Tabla))+getTableName(Tabla)+" WHERE "+condicion.parse();
        int rows = select(sql,new Result<Integer>()
        {
            @Override
            public Integer handleResults(ResultSet resultSet) throws EjecutionDBExcepcion
            {
                try
                {
                    resultSet.next();
                    return resultSet.getInt(1);
                }
                catch (SQLException ex)
                {
                    throw new EjecutionDBExcepcion(ex);
                }
            }
        });
        return rows;
    }
    
    public boolean existsTable(final Class table) throws EjecutionDBExcepcion
    {
        if(connection == null)
            throw new EjecutionDBExcepcion("No se ha establecido la conexion. connect()");
        //SELECT table_name FROM information_schema.tables WHERE table_name ilike 'EnTidad' AND table_type='BASE TABLE'
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_name ilike '"+getPrefix(table)+getTableName(table) +"' AND table_type='BASE TABLE'";
        boolean result =  select(sql,new Result<Boolean>()
        {
            @Override
            public Boolean handleResults(ResultSet resultSet) throws EjecutionDBExcepcion
            {
                try
                {
                    return resultSet.next();
                }
                catch (SQLException e)
                {
                    throw new EjecutionDBExcepcion(e);
                }
            }
        });
        return result;
    }

    public <U> boolean existsTable(U Tabla) throws EjecutionDBExcepcion
    {
        return existsTable(getTableName(Tabla));
    }
}