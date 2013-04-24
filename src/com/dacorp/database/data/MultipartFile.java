package com.dacorp.database.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Ronald
 */
public class MultipartFile
{
    private String name;
    private String path;
    private String contentType;
    private long length;
    public static char ENCODING_SEPARATOR = '%';

    public MultipartFile()
    {
    }

    @Override
    public String toString()
    {
        return "MultipartFile{" + "name=" + name + ", path=" + path + ", contentType=" + contentType + '}';
    }

    public void setLength(long length) {
        this.length = length;
    }

    public long getLength() {
        return length;
    }

    public String getContentType()
    {
        return contentType;
    }

    public void setContentType(String contentType)
    {
        this.contentType = contentType;
    }
    
    public InputStream getInpupStream() throws IOException
    {
        try
        {
            if(exists())
                return new FileInputStream(path);
        }
        catch (Exception e){}
        return null;
    }
    
    public boolean exists()
    {
        File file = new File(path);
        return file.exists();
    }
    
    public void delete()
    {
        File file = new File(path);
        file.deleteOnExit();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}