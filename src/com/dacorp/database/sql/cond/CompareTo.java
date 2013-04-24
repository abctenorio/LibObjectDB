package com.dacorp.database.sql.cond;

/**
 * 
 * @author Coarite Mamani Ronald
 * @version 1.0, Compilado con  Netbeans 6.9
 */
public abstract class CompareTo extends Condition
{
    protected String attribute;
    protected Object value;
    /**
     * This attribute refers to the comparator symbol used fo the filter,
     * it may be: '=', '<', etc
     */
    private String COMPARATOR;
    /** Default constructor */
    CompareTo(String comparator, String attribute, Object value)
    {
        super("");
        this.COMPARATOR = comparator;
        this.attribute = attribute;
        this.value = value;
    }
    
    protected String formatValue()
    {
        String text = String.valueOf(value);
        if(text.toLowerCase().startsWith("select"))
        {
            return text;
        }
        else
        {
            return "'" + text + "'";
        }
    }

    @Override
    public final String parse()
    {
        return attribute + COMPARATOR + formatValue();
    }
}