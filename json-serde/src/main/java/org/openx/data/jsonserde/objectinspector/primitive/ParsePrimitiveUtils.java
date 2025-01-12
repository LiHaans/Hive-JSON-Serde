/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.openx.data.jsonserde.objectinspector.primitive;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang.StringUtils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 *
 * @author rcongiu
 */
public final class ParsePrimitiveUtils {

    private ParsePrimitiveUtils() {
        throw new InstantiationError("This class must not be instantiated.");
    }

    // check to see if UTC_WITH_MILLIS_FORMAT should be used.
    private static final Pattern UTC_MILLIS_PATTERN = Pattern.compile(".*\\.[0-9]{3}Z$");

    // timestamps are expected to be in UTC
    public final static DateFormat UTC_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    public final static DateFormat UTC_WITH_MILLIS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    public final static DateFormat OFFSET_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
    public final static DateFormat NON_UTC_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static DateFormat[] dateFormats = { UTC_FORMAT, UTC_WITH_MILLIS_FORMAT, OFFSET_FORMAT,NON_UTC_FORMAT};
    static {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        for( DateFormat df : dateFormats) {
            df.setTimeZone(tz);
        }
    }

    static Pattern hasTZOffset = Pattern.compile(".+(\\+|-)\\d{2}:?\\d{2}$");

    public static boolean isHex(String s) {
        return s.startsWith("0x") || s.startsWith("0X");
    }

    public static byte parseByte(String s) {
        if (isHex(s)) {
            return Byte.parseByte(s.substring(2), 16);
        } else {
            return Byte.parseByte(s);
        }
    }

    public static int parseInt(String s) {
        if (isHex(s)) {
            return Integer.parseInt(s.substring(2), 16);
        } else {
            return Integer.parseInt(s);
        }
    }

    public static short parseShort(String s) {
        if (isHex(s)) {
            return Short.parseShort(s.substring(2), 16);
        } else {
            return Short.parseShort(s);
        }
    }

    public static long parseLong(String s) {
        if (isHex(s)) {
            return Long.parseLong(s.substring(2), 16);
        } else {
            return Long.parseLong(s);
        }
    }

    static TimeZone defaultZone = TimeZone.getDefault();
    public static String serializeAsUTC(Timestamp ts) {
        return UTC_FORMAT.format(ts.getTime() );
    }
    public static String serializeAsNoUTC(Timestamp ts) {
        return NON_UTC_FORMAT.format(ts.getTime() );
    }

    public static Timestamp parseTimestamp(String s) {
        final String sampleUnixTimestampInMs = "1454612111000";

        Timestamp value;
        if (s.indexOf(':') > 0) {
            value = Timestamp.valueOf(nonUTCFormat(s));
        } else if (s.indexOf('.') >= 0) {
            // it's a float
            value = new Timestamp(
                    (long) ((double) (Double.parseDouble(s) * 1000)));
        } else {
            // integer 
            Long timestampValue = Long.parseLong(s);
            Boolean isTimestampInMs = s.length() >= sampleUnixTimestampInMs.length();
            if(isTimestampInMs) {
                value = new Timestamp(timestampValue);
            } else {
                value = new Timestamp(timestampValue * 1000);
            }            
        }
        return value;
    }

    /**
     * Timestamp.parse gets an absolute time, without the timezone.
     * This function translates to the right string format that Timestamp
     * can parse.
     *
     * @param s
     * @return
     */
    public static String nonUTCFormat(String s) {
        Date parsed = null;
        try {
            if(s.endsWith("Z")) { // 003Z
                if (UTC_MILLIS_PATTERN.matcher(s).matches()) {
                    parsed = UTC_WITH_MILLIS_FORMAT.parse(s);
                } else {
                    parsed = UTC_FORMAT.parse(s);
                }
            } else if ( hasTZOffset.matcher(s).matches()) {
                parsed = OFFSET_FORMAT.parse(s); // +0600 or -06:00
            } else {
                return s;
            }

        } catch (ParseException e) {
                e.printStackTrace();
        }

        if(parsed!=null) {
            return NON_UTC_FORMAT.format(parsed);
        } else {
            return s;
        }
    }


    public static ZonedDateTime parseZonedDateTime(String s) {
       return null;

    }
}
