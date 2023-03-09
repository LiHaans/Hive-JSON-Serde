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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;

/**
 * This String ObjectInspector was built to tolerate non-string values 
 * and treat them as strings. 
 * @author rcongiu
 */
public class JsonStringCharInspector extends AbstractPrimitiveJavaObjectInspector implements SettableHiveCharObjectInspector {

  public JsonStringCharInspector() {
    super(TypeEntryShim.charType);
  }


  @Override
  public Object set(Object o, HiveChar hiveChar) {
    return hiveChar == null ? null : hiveChar.toString();
  }

  @Override
  public Object set(Object o, String s) {
    return s;
  }

  @Override
  public Object create(HiveChar hiveChar) {
    return hiveChar == null ? null : hiveChar.toString();
  }

  @Override
  public HiveCharWritable getPrimitiveWritableObject(Object o) {
    if(o == null) return null;

    if(o instanceof String) {
      HiveChar hiveChar = new HiveChar();
      hiveChar.setValue(o.toString());
      return new HiveCharWritable(hiveChar);
    } else {
      return new HiveCharWritable((HiveChar) o);
    }
  }

  @Override
  public HiveChar getPrimitiveJavaObject(Object o) {
    HiveChar hiveChar = new HiveChar();
    hiveChar.setValue(o.toString());
    return hiveChar;
  }
}
