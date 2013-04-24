package com.dacorp.database.sql;

import com.dacorp.database.annot.Column;
import com.dacorp.database.data.Binari;
import com.dacorp.database.data.Location;
import com.dacorp.database.data.MultipartFile;
import com.dacorp.database.error.EjecutionDBExcepcion;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import org.postgis.PGgeometry;
import org.postgis.Point;

/**
 * @author  Coarite Mamani Ronald
 * @version 1.0, Compilado con Netbeans 6.8
 */
public class ResultReader
{
    public static <T> T doMap(ResultSet resultSet,Class<T> clase) throws EjecutionDBExcepcion
    {
        try
        {
            T objeto_instancia = clase.newInstance();
            ArrayList<Field> fields = new ArrayList<Field>(10);
            recolectarFields(fields, clase);
            for (Field field : fields)
            {
                String metName = getSetterName(field.getName());
                Method method = null;
                try
                {
                    // Metodo set del Objeto
                    method = clase.getMethod(metName, field.getType());
                }
                catch (NoSuchMethodException ex)
                {
                    throw new EjecutionDBExcepcion("No se encuentra el metodo ["+ResultReader.getSetterName(field)+"("+field.getName()+" atributo)] en la clase["+clase.getSimpleName()+"]");
                } 
                catch (SecurityException ex)
                {
                    continue;
                }
                Object valor_columna;
                try
                {
                    int indice_columna;
                    try
                    {
                        indice_columna = resultSet.findColumn(field.getName());
                        if (indice_columna == -1)
                            continue;
                    }
                    catch (Exception e)
                    {
                        continue;
                    }
                    // Obtenemos el valor del result set en un Objeto
                    valor_columna = leerValorColumna(resultSet, field.getType(), indice_columna);
                    // Llamamos al metodo set del objeto instanciado
                    method.invoke(objeto_instancia, valor_columna);
                }
                catch (IllegalArgumentException ex)
                {

                } 
                catch (InvocationTargetException ex)
                {
                    
                }
            }
            return objeto_instancia;
        } 
        catch (InstantiationException ex)
        {
            throw new EjecutionDBExcepcion("El objeto ["+clase.getName()+"] no tiene un constructor sin parametros. inserte ["+clase.getSimpleName()+"(){}]", ex);
        }
        catch (IllegalAccessException ex)
        {
            throw new EjecutionDBExcepcion("No se puede accecer a los metodos del objeto ["+clase.getName()+"]", ex);
        }
        catch(SQLException sqle)
        {
            sqle.printStackTrace();
            throw new EjecutionDBExcepcion(sqle);
        }
    }
    
    public static void recolectarFields(List<Field> atributos,Class clase)
    {
        String nombre = clase.getSimpleName();
        if(nombre.equals(Object.class.getSimpleName()))
            return;
        for (Field field : clase.getDeclaredFields())
        {
            // si es un atributo procesamos para crear separadamente
            Column columna = field.getAnnotation(Column.class);
            if(columna != null)
                atributos.add(field);
        }
        recolectarFields(atributos,clase.getSuperclass());
    }
    
    public static List<Field> recolectarFields(Class clase)
    {
        List<Field> atributos = new ArrayList<Field>(5);
        for (Field field : clase.getDeclaredFields())
        {
            // si es un atributo procesamos para crear separadamente
            Column columna = field.getAnnotation(Column.class);
            if(columna != null)
                atributos.add(field);
        }
        return atributos;
    }

    /*
    public static Object leerValorColumna(ResultSet resultSet, Class type, String colName) throws SQLException, EjecutionDBExcepcion
    {
        return leerValorColumna(resultSet, type, resultSet.findColumn(colName));
    }*/

    /*
    public static Object leerValorColumna(ResultSet cursor, Field field) throws SQLException, EjecutionDBExcepcion
    {
        //Column col = field.getAnnotation(Column.class);
        int idx = cursor.findColumn(field.getName());
        return leerValorColumna(cursor, field.getType(), idx);
    }*/
    
    public static <T> T comvertType(Object object,Field field) throws EjecutionDBExcepcion
    {
        Class clase = field.getType();
        if(object == null)
        {
            if(clase.isAssignableFrom(BufferedImage.class)
                    ||clase.isAssignableFrom(Image.class)
                    ||clase.isAssignableFrom(String.class)
                    ||clase.isAssignableFrom(Binari.class)
                    ||clase.isAssignableFrom(Location.class)
                    ||clase.isAssignableFrom(MultipartFile.class))
                return null;
            return (T)new Integer(0);
        }

        Class clase_obj = object.getClass();
        if(!clase.isAssignableFrom(clase_obj))
        {
            throw new EjecutionDBExcepcion("El tipo de dato que desea obtener ["+clase_obj.getSimpleName()+"] no es compatible con el tipo ["+clase.getSimpleName()+"] de la columa y en la tabla");
        }
        else
        {
            try
            {
                return (T)object;
            }
            catch (ClassCastException e)
            {
                throw new EjecutionDBExcepcion("El tipo de dato del resultado ["+clase_obj.getSimpleName()+"] no es del mismo tipo al que se quiere asignar",e);
            }
        }
    }

    public static Object leerValorColumna(ResultSet resultSet, Class type, int indice_columna) throws SQLException, EjecutionDBExcepcion
    {
        Object value = null;
        if (Boolean.class.isAssignableFrom(type)
                || boolean.class.isAssignableFrom(type))
        {
            value = resultSet.getBoolean(indice_columna);
        }
        else if(Double.class.isAssignableFrom(type)
                || double.class.isAssignableFrom(type))
        {
            value = resultSet.getDouble(indice_columna);
        }
        else if (Integer.class.isAssignableFrom(type)
                || int.class.isAssignableFrom(type))
        {
            value = resultSet.getInt(indice_columna);
        }
        else if (Float.class.isAssignableFrom(type)
                || float.class.isAssignableFrom(type))
        {
            value = resultSet.getFloat(indice_columna);
        }
        else if (Long.class.isAssignableFrom(type)
                || long.class.isAssignableFrom(type)) 
        {
            value = resultSet.getLong(indice_columna);
        }
        else if (String.class.isAssignableFrom(type))
        {
            value = resultSet.getString(indice_columna);
        }
        else if (BufferedImage.class.isAssignableFrom(type)
                ||Image.class.isAssignableFrom(type))
        {
            try
            {
                byte[] bytes = resultSet.getBytes(indice_columna);
                if(bytes!=null)
                {
                    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                    value = javax.imageio.ImageIO.read(bais);
                }
            }
            catch (IOException ex)
            {
                throw new EjecutionDBExcepcion("No se puede leer imagen",ex);
            }
        }
        else if(Binari.class.isAssignableFrom(type))
        {
            try
            {
                byte[] bytes = resultSet.getBytes(indice_columna);
                if(bytes!=null)
                {
                    InputStream input = new ByteArrayInputStream(bytes);
                    Binari binario = new Binari(input);
                    value = binario;
                }
            }
            catch (Exception ex)
            {
                throw new EjecutionDBExcepcion("No se puede leer el FlujoBinario",ex);
            }
        }
        else if(Date.class.isAssignableFrom(type))
        {
            String fecha = resultSet.getString(indice_columna);
            if(fecha == null)
            	value = null;
            else
            {
            	SimpleDateFormat formatoDelTexto = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date fechaEnviar = null;
                try
                {
                    fechaEnviar = formatoDelTexto.parse(fecha);
                    value = fechaEnviar;
                }
                catch (ParseException e)
                {
                    value = null;
                }
            }
        }
        else if(Location.class.isAssignableFrom(type))
        {
            try
            {
                PGgeometry pGgeometry = (PGgeometry)resultSet.getObject(indice_columna);
                if(pGgeometry == null)
                {
                    value = null;
                }
                else
                {
                    Point point = (Point)pGgeometry.getGeometry();
                    Location location = new Location(point.getX(),point.getY(),point.getZ());
                    value = location;
                }
            }
            catch (Exception ex)
            {
                throw new EjecutionDBExcepcion("Error en la consulta",ex);
            }
        }
        else if(MultipartFile.class.isAssignableFrom(type))
        {
            String propiedades = resultSet.getString(indice_columna);
            if(propiedades == null)
            {
                value = null;
            }
            else
            {
                try
                {
                    StringTokenizer tokenizer = new StringTokenizer(
                            propiedades,
                            String.valueOf(MultipartFile.ENCODING_SEPARATOR));
                    MultipartFile multipartFile = new MultipartFile();
                    multipartFile.setName(tokenizer.nextToken());
                    multipartFile.setPath(tokenizer.nextToken());
                    multipartFile.setContentType(tokenizer.nextToken());
                    multipartFile.setLength(Long.parseLong(tokenizer.nextToken()));
                    value = multipartFile;
                }
                catch (Exception e)
                {
                    value = null;
                }
            }
        }
        else
        {
            try
            {
                // Se asume que es un objeto que implementa la interfaz Serializable
                // y se guarda el objeto
                Blob blob = resultSet.getBlob(indice_columna);
                if(blob != null)
                {
                    ObjectInputStream ois = new ObjectInputStream(blob.getBinaryStream());
                    value = ois.readObject();
                    ois.close();
                }
            }
            catch (IOException ex)
            {
                value = null;
            }
            catch(ClassNotFoundException cnfe)
            {
                value = null;
            }
        }
        return value;
    }

    public static void addValor(
            PreparedStatement preparedStatement,
            int posicion_columna,
            Object valor) throws EjecutionDBExcepcion
    {
        try
        {
            if(valor==null)
            {
                preparedStatement.setObject(posicion_columna,null);
                return;
            }
            Class type = valor.getClass();
            if (String.class.isAssignableFrom(type))
            {
                preparedStatement.setString(posicion_columna,String.valueOf(valor));
            }
            else if (Boolean.class.isAssignableFrom(type) || boolean.class.isAssignableFrom(type))
            {
                preparedStatement.setBoolean(posicion_columna,Boolean.valueOf(valor+""));
            }
            else if (Double.class.isAssignableFrom(type)|| double.class.isAssignableFrom(type))
            {
                preparedStatement.setDouble(posicion_columna,Double.parseDouble(valor+""));
            }
            else if (Float.class.isAssignableFrom(type)|| float.class.isAssignableFrom(type))
            {
                preparedStatement.setFloat(posicion_columna,Float.parseFloat(valor+""));
            }
            else if (Integer.class.isAssignableFrom(type)|| int.class.isAssignableFrom(type))
            {
                preparedStatement.setInt(posicion_columna,Integer.parseInt(valor+""));
            }
            else if (Short.class.isAssignableFrom(type)|| short.class.isAssignableFrom(type))
            {
                preparedStatement.setShort(posicion_columna,Short.parseShort(valor+""));
            }
            else if (Long.class.isAssignableFrom(type)|| long.class.isAssignableFrom(type))
            {
                preparedStatement.setLong(posicion_columna,Long.parseLong(valor+""));
            }
            else if (BufferedImage.class.isAssignableFrom(type)||Image.class.isAssignableFrom(type))
            {
                try
                {
                    BufferedImage img = null;
                    if(valor instanceof Image)
                    {
                        Image image = (Image)valor;
                        img = new BufferedImage(image.getWidth(null),image.getHeight(null),BufferedImage.BITMASK);
                        Graphics2D g2 = img.createGraphics();
                        g2.drawImage(image, 0, 0, null);
                        g2.dispose();
                    }
                    else
                        img = (BufferedImage)valor;
                    
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    javax.imageio.ImageIO.write(img, "PNG", baos);
                    //ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                    preparedStatement.setBytes(posicion_columna, baos.toByteArray());
                    //preparedStatement.setBlob(posicion_columna, bais);
                }
                catch (IOException ex)
                {
                    throw new EjecutionDBExcepcion("Error en la conversion de imagen",ex);
                }
            }
            else if(Date.class.isAssignableFrom(type))
            {
                Date date = (Date)valor;
                Timestamp timestamp = new Timestamp(date.getTime());
                preparedStatement.setTimestamp(posicion_columna, timestamp);
            }
            else if(Location.class.isAssignableFrom(type))
            {
                Location location = (Location)valor;
                Point point = new Point(location.getLongitude(),location.getLatitude(), location.getAltitude());
                PGgeometry pGgeometry = new PGgeometry();
                pGgeometry.setGeometry(point);
                preparedStatement.setObject(posicion_columna, pGgeometry);
            }
            else if(Binari.class.isAssignableFrom(type))
            {
                try
                {
                    Binari binario = (Binari) valor;
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    binario.escribir(baos);
                    preparedStatement.setBytes(posicion_columna, baos.toByteArray());
                }
                catch (Exception ex)
                {
                    throw new EjecutionDBExcepcion("No se puede leer el archivo",ex);
                }
            }
            else if(MultipartFile.class.isAssignableFrom(type))
            {
                try
                {
                    MultipartFile binario = (MultipartFile) valor;
                    StringBuilder builder = new StringBuilder();
                    builder.append(binario.getName());
                    builder.append(MultipartFile.ENCODING_SEPARATOR);
                    builder.append(binario.getPath());
                    builder.append(MultipartFile.ENCODING_SEPARATOR);
                    builder.append(binario.getContentType());
                    builder.append(MultipartFile.ENCODING_SEPARATOR);
                    builder.append(binario.getLength());
                    //System.out.println(ResultReader.class.getName()+":"+builder.toString());
                    //preparedStatement.setString(posicion_columna,String.valueOf(builder.toString().getBytes("UTF-8")));
                    preparedStatement.setString(posicion_columna,builder.toString());
                }
                catch (Exception ex)
                {
                    throw new EjecutionDBExcepcion("No se puede leer el archivo",ex);
                }
            }
            else
            {
                try
                {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(valor);
                    oos.close();
                    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
                    preparedStatement.setBlob(posicion_columna, bais);
                }
                catch (IOException ex)
                {
                    throw new EjecutionDBExcepcion("Error al establecer objeto: "+valor.getClass().getSimpleName(),ex);
                }
            }
        }
        catch(EjecutionDBExcepcion ejeEx)
        {
            throw ejeEx;
        }
        catch(SQLException exception)
        {
            throw  new EjecutionDBExcepcion(exception);
        }
    }

    public static void addSQLTipo(StringBuilder create,Column columna,Class type,String nombre_columna)
    {
        if(columna.auto_incrementing())
        {
            //serial NOT NULL
            create.append(nombre_columna);
            create.append(" serial NOT NULL");
            create.append(",");
            return;
        }
        if (Double.class.isAssignableFrom(type)
                || double.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" REAL,");
        }
        else if(Float.class.isAssignableFrom(type)
                || float.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" REAL,");
        }
        else if(Boolean.class.isAssignableFrom(type)
                || boolean.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" BOOL,");
        }
        else if (Integer.class.isAssignableFrom(type)
                || int.class.isAssignableFrom(type)
                || Short.class.isAssignableFrom(type)
                || short.class.isAssignableFrom(type)
                || Long.class.isAssignableFrom(type)
                || long.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" INTEGER");
            //create.append(columna.primary_key() ? " PRIMARY KEY" : "");
            //create.append(columna.auto_incrementing() ? " AUTO_INCREMENT" : "");
            create.append(",");
        }
        else if (String.class.isAssignableFrom(type)
                || StringBuffer.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" TEXT,");
        }
        else if (Image.class.isAssignableFrom(type)
                || BufferedImage.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" BYTEA,");
        }
        else if(Date.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" TIMESTAMP,");
        }
        else if(Location.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" GEOMETRY,");
        }        
        else if(Binari.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" BYTEA,");
        }
        else if (MultipartFile.class.isAssignableFrom(type))
        {
            create.append(nombre_columna);
            create.append(" TEXT,");
        }
        else
        {
            create.append(nombre_columna);
            create.append(" BYTEA,");
        }
    }

    public static String getGetterName(Field field)
    {
        String fieldName = field.getName();
        return getGetterName(fieldName, field.getType());
    }

    public static String getGetterName(String fieldName, Class type)
    {

        String getterName = (type == Boolean.class || type == boolean.class) ? "is" : "get";
        getterName += fieldName.substring(0, 1).toUpperCase();
        getterName += fieldName.substring(1, fieldName.length());
        return getterName;
    }

    public static String getSetterName(Field field)
    {
        String fieldName = field.getName();
        return getSetterName(fieldName);
    }

    public static String getSetterName(String fieldName)
    {
        String setterName = "set";
        setterName += fieldName.substring(0, 1).toUpperCase();
        setterName += fieldName.substring(1, fieldName.length());
        return setterName;
    }
}