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

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;

/**
 * This String ObjectInspector was built to tolerate non-string values 
 * and treat them as strings. 
 * @author rcongiu
 */
public class JsonStringVarcharInspector extends AbstractPrimitiveJavaObjectInspector implements SettableHiveVarcharObjectInspector {

  public JsonStringVarcharInspector() {
    super(TypeEntryShim.varcharType);
  }


  @Override
  public Object set(Object o, HiveVarchar hiveVarchar) {
    return hiveVarchar == null ? null : hiveVarchar.toString();
  }

  @Override
  public Object set(Object o, String s) {
    return s;
  }

  @Override
  public Object create(HiveVarchar hiveVarchar) {
    return hiveVarchar == null ? null : hiveVarchar.toString();
  }

  @Override
  public HiveVarcharWritable getPrimitiveWritableObject(Object o) {
    if(o == null) return null;

    if(o instanceof String) {
      HiveVarchar hiveVarchar = new HiveVarchar();
      hiveVarchar.setValue(o.toString());
      return new HiveVarcharWritable(hiveVarchar);
    } else {
      return new HiveVarcharWritable((HiveVarchar) o);
    }
  }

  @Override
  public HiveVarchar getPrimitiveJavaObject(Object o) {
    HiveVarchar hiveVarchar = new HiveVarchar();
    hiveVarchar.setValue(o.toString());
    return hiveVarchar;
  }
}
