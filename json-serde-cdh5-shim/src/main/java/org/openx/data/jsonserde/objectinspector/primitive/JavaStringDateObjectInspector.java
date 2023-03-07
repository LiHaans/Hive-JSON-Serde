package org.openx.data.jsonserde.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;

import java.sql.Date;
import java.text.SimpleDateFormat;

/**
 * Created by rcongiu on 11/12/15.
 */
public class JavaStringDateObjectInspector  extends AbstractPrimitiveJavaObjectInspector
        implements SettableDateObjectInspector {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public JavaStringDateObjectInspector() {
        super(TypeEntryShim.dateType);
    }


    @Override
    public Object set(Object o, Date date) {
        return date.toString();
    }

    @Override
    public Object set(Object o, org.apache.hadoop.hive.common.type.Date d) {
        return d.toString();
    }

    @Override
    public Object set(Object o, DateWritableV2 tw) {
        return create( tw.get());
    }

    @Override
    public Object create(Date d) {
        return d.toString();
    }


    @Override
    public Object create(org.apache.hadoop.hive.common.type.Date d) {
        return d.toString();
    }

    @Override
    public DateWritableV2 getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
            Date parse = parse((String) o);
            org.apache.hadoop.hive.common.type.Date date = new org.apache.hadoop.hive.common.type.Date();
            date.setTimeInMillis(parse.getTime());
            return new DateWritableV2(date);
        } else {
            return new DateWritableV2((org.apache.hadoop.hive.common.type.Date) o);
        }
    }

    @Override
    public org.apache.hadoop.hive.common.type.Date getPrimitiveJavaObject(Object o) {
        if(o instanceof String) {
            Date parse = parse((String) o);
            org.apache.hadoop.hive.common.type.Date date = new org.apache.hadoop.hive.common.type.Date();
            date.setTimeInMillis(parse.getTime());
           return date;
        } else {
            if (o instanceof Date) return (org.apache.hadoop.hive.common.type.Date) o;
        }
        return null;
    }

    protected java.sql.Date parse(String o) {
        try {
            return new java.sql.Date(sdf.parse(o).getTime());
        } catch(java.text.ParseException e) {
            return null;
        }
    }

}