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

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * A timestamp that is stored in a String
 * @author rcongiu
 */
public class JavaStringTimestampLocalTzObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements SettableTimestampLocalTZObjectInspector {

    public JavaStringTimestampLocalTzObjectInspector() {
        super(TypeEntryShim.timestampTzType);
    }


    @Override
    public Object set(Object o, byte[] bytes, int offset) {
        return create(bytes, offset);
    }

    @Override
    public Object set(Object o, TimestampTZ t) {
        return t.toString();
    }

    @Override
    public Object set(Object o, TimestampLocalTZWritable t) {
        return t.getTimestampTZ().toString();
    }

    @Override
    public Object create(byte[] bytes, int offset) {
        //return formatTimeStamp(new TimestampLocalTZWritable(bytes, offset));
        TimestampTZ utc = new TimestampLocalTZWritable(bytes, offset, ZoneId.of("UTC")).getTimestampTZ();
        return utc.toString();
    }

    @Override
    public Object create(TimestampTZ t) {
        return t.toString();
    }

    @Override
    public TimestampLocalTZWritable getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
            ZonedDateTime zonedDateTime = ParsePrimitiveUtils.parseZonedDateTime((String) o);
            TimestampTZ hiveTimestamp = new TimestampTZ();
            hiveTimestamp.setZonedDateTime(zonedDateTime);
            return new TimestampLocalTZWritable(hiveTimestamp);
        } else {
            return new TimestampLocalTZWritable((TimestampTZ) o);
        }
    }

    @Override
    public TimestampTZ getPrimitiveJavaObject(Object o) {
        return null;
    }

}
