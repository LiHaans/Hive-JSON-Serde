/*======================================================================*
 * Copyright (c) 2011, OpenX Technologies, Inc. All rights reserved.    *
 *                                                                      *
 * Licensed under the New BSD License (the "License"); you may not use  *
 * this file except in compliance with the License. Unless required     *
 * by applicable law or agreed to in writing, software distributed      *
 * under the License is distributed on an "AS IS" BASIS, WITHOUT        *
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     *
 * See the License for the specific language governing permissions and  *
 * limitations under the License. See accompanying LICENSE file.        *
 *======================================================================*/

package org.openx.data.jsonserde.objectinspector.primitive;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;

/**
 * A timestamp that is stored in a String
 * @author rcongiu
 */
public class JavaStringTimestampObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements SettableTimestampObjectInspector {
    
    public JavaStringTimestampObjectInspector() {
        super(TypeEntryShim.timestampType);
    }

    
    @Override
    public Object set(Object o, byte[] bytes, int offset) {
        return create(bytes,offset);
    }

    @Override
    public Object set(Object o, Timestamp tmstmp) {
        return tmstmp.toString();
    }

    @Override
    public Object set(Object o, org.apache.hadoop.hive.common.type.Timestamp timestamp) {
        return null;
    }

    @Override
    public Object set(Object o, TimestampWritableV2 timestampWritableV2) {
        return null;
    }


    @Override
    public Object create(byte[] bytes, int offset) {
       return formatTimeStamp(new TimestampWritable(bytes, offset));
    }

    @Override
    public Object create(Timestamp tmstmp) {
        return formatTimeStamp(tmstmp);
    }

    @Override
    public Object create(org.apache.hadoop.hive.common.type.Timestamp timestamp) {
        return null;
    }

    private String formatTimeStamp(Timestamp ts) {
        //return ParsePrimitiveUtils.serializeAsUTC(ts);
        return ParsePrimitiveUtils.serializeAsNoUTC(ts);
    }

    private String formatTimeStampTz(Timestamp ts) {
        //return ParsePrimitiveUtils.serializeAsUTC(ts);
        return ParsePrimitiveUtils.serializeAsUTC(ts);
    }

    private String formatTimeStamp(TimestampWritable tsw) {
        return formatTimeStamp(tsw.getTimestamp());
    }

    @Override
    public TimestampWritableV2 getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
            Timestamp timestamp = ParsePrimitiveUtils.parseTimestamp((String) o);

            org.apache.hadoop.hive.common.type.Timestamp hiveTimestamp = new org.apache.hadoop.hive.common.type.Timestamp();
            hiveTimestamp.setTimeInMillis(timestamp.getTime());
            return new TimestampWritableV2(hiveTimestamp);
        } else {
          return new TimestampWritableV2((org.apache.hadoop.hive.common.type.Timestamp) o);
        }
    }

    @Override
    public org.apache.hadoop.hive.common.type.Timestamp getPrimitiveJavaObject(Object o) {
         if(o instanceof String) {
             Timestamp timestamp = ParsePrimitiveUtils.parseTimestamp((String) o);
             org.apache.hadoop.hive.common.type.Timestamp hiveTimestamp = new org.apache.hadoop.hive.common.type.Timestamp();
             hiveTimestamp.setTimeInMillis(timestamp.getTime());
             return hiveTimestamp;
        } else {
           return (org.apache.hadoop.hive.common.type.Timestamp) o;
        }
    }

   
}
