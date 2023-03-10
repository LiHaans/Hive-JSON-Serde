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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;


/**
 * A timestamp that is stored in a String
 * @author rcongiu
 */
public class JavaStringDecimalObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements SettableHiveDecimalObjectInspector {

    public JavaStringDecimalObjectInspector() {
        super(TypeEntryShim.decimalType);
    }


    @Override
    public Object set(Object o, byte[] bytes, int i) {
        return new HiveDecimalWritable(bytes, i).getHiveDecimal();
    }

    @Override
    public Object set(Object o, HiveDecimal hiveDecimal) {
        return hiveDecimal;
    }

    @Override
    public Object set(Object o, HiveDecimalWritable hiveDecimalWritable) {
        return hiveDecimalWritable.getHiveDecimal();
    }

    @Override
    public Object create(byte[] bytes, int i) {
        return new HiveDecimalWritable(bytes, i).getHiveDecimal();
    }

    @Override
    public Object create(HiveDecimal hiveDecimal) {
        return hiveDecimal;
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
        if(o == null) return null;

        if(o instanceof String) {
            return new HiveDecimalWritable((HiveDecimal.create((String)o)));
        } else {
            return new HiveDecimalWritable((HiveDecimal) o);
        }
    }


    @Override
    public HiveDecimal getPrimitiveJavaObject(Object var1) {
        return get(var1);
    }

    public HiveDecimal get(Object o) {

        if(o instanceof String) {
            return HiveDecimal.create((String)o);
        } else {
            return (HiveDecimal) o;
        }
    }
}
