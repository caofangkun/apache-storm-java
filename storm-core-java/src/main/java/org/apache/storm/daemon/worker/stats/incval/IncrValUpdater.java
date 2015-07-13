/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.daemon.worker.stats.incval;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.stats.rolling.UpdateParams;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.stats#incr-val")
public class IncrValUpdater extends RunnableCallback {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    Map<Object, Long> curr = null;
    if (args != null && args.length > 0) {
      UpdateParams p = (UpdateParams) args[0];
      if (p.getCurr() != null) {
        curr = (Map<Object, Long>) p.getCurr();
      } else {
        curr = new HashMap<Object, Long>();
      }
      Object[] incArgs = p.getArgs();

      Long amt = 1l;

      if (incArgs.length > 1) {
        amt = Long.parseLong(String.valueOf(incArgs[1]));
      }
      incr_val(curr, incArgs[0], amt);

    }
    return curr;
  }

  @ClojureClass(className = "backtype.storm.stats#incr-val")
  private static void incr_val(Map<Object, Long> amap, Object key, Long amt) {
    Long val = Long.valueOf(0);
    if (amap.containsKey(key)) {
      val = amap.get(key);
    }
    val = (Long) CoreUtil.add(val, amt);
    amap.put(key, val);
  }

  @ClojureClass(className = "backtype.storm.stats#incr-val")
  private static void incr_val(Map<Object, Long> amap, Object key) {
    incr_val(amap, key, Long.valueOf(1));
  }

}
