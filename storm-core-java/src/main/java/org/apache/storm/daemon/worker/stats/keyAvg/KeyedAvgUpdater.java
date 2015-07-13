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
package org.apache.storm.daemon.worker.stats.keyAvg;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.stats.Pair;
import org.apache.storm.daemon.worker.stats.rolling.UpdateParams;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.stats#update-keyed-avg")
public class KeyedAvgUpdater extends RunnableCallback {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    Map<Object, Pair<Long, Long>> curr = null;
    if (args != null && args.length > 0) {
      UpdateParams p = (UpdateParams) args[0];
      if (p.getCurr() != null) {
        curr = (Map<Object, Pair<Long, Long>>) p.getCurr();
      } else {
        curr = new HashMap<Object, Pair<Long, Long>>();
      }
      Object[] keyAvgArgs = p.getArgs();

      Long amt = 1l;
      if (keyAvgArgs.length > 1) {
        amt = Long.parseLong(String.valueOf(keyAvgArgs[1]));
      }
      update_keyed_avg(curr, keyAvgArgs[0], amt);
    }
    return curr;
  }

  @ClojureClass(className = "backtype.storm.stats#update-keyed-avg")
  private static void update_keyed_avg(Map<Object, Pair<Long, Long>> map,
      Object key, long val) {
    Pair<Long, Long> p = map.get(key);
    if (p == null) {
      p = new Pair<Long, Long>(0l, 0l);
    }
    update_avg(p, val);
    map.put(key, p);
  }

  @ClojureClass(className = "backtype.storm.stats#update-avg")
  private static Pair<Long, Long> update_avg(Pair<Long, Long> curr, long val) {
    if (curr != null) {
      curr.setFirst(curr.getFirst() + val);
      curr.setSecond(curr.getSecond() + 1);
    } else {
      curr = new Pair<Long, Long>(val, 1l);
    }
    return curr;
  }
}
