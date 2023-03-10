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

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.nio.charset.CharacterCodingException;
import java.sql.Timestamp;

/**
 * A timestamp that is stored in a String
 * @author rcongiu
 */
public class JavaStringBinaryObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements SettableBinaryObjectInspector {

    public JavaStringBinaryObjectInspector() {
        super(TypeEntryShim.binaryType);
    }


    @Override
    public Object set(Object o, byte[] bytes) {
        return bytes.toString();
    }

    @Override
    public Object set(Object o, BytesWritable bytesWritable) {
        return bytesWritable.getBytes().toString();
    }

    @Override
    public Object create(byte[] bytes) {
        return bytes.toString();
    }

    @Override
    public Object create(BytesWritable bytesWritable) {
        return bytesWritable.getBytes().toString();
    }

    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
            return new BytesWritable(((String)o).getBytes());
        } else {
            return new BytesWritable((byte[]) o);
        }
    }

    @Override
    public byte[] getPrimitiveJavaObject(Object o){
        if (o == null) return null;

        if (o instanceof String) {
            return ((String) o).getBytes();
        }

        return (byte[])o;
    }
}
