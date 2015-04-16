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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.stats.Pair;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.stats#merge-keyed-avg")
public class KeyedAvgMerger extends RunnableCallback {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    List<Map<Object, Pair<Long, Long>>> list =
        (List<Map<Object, Pair<Long, Long>>>) args[0];

    Map<Object, Pair<Long, Long>> result =
        new HashMap<Object, Pair<Long, Long>>();

    Map<Object, List<Pair<Long, Long>>> trans =
        new HashMap<Object, List<Pair<Long, Long>>>();

    for (Map<Object, Pair<Long, Long>> each : list) {

      for (Entry<Object, Pair<Long, Long>> e : each.entrySet()) {

        Object key = e.getKey();
        List<Pair<Long, Long>> val = trans.get(key);
        if (val == null) {
          val = new ArrayList<Pair<Long, Long>>();
        }
        val.add(e.getValue());
        trans.put(key, val);
      }
    }

    for (Entry<Object, List<Pair<Long, Long>>> e : trans.entrySet()) {
      result.put(e.getKey(), merge_keyed_avg(e.getValue()));
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.stats#merge-keyed-avg")
  private static Pair<Long, Long> merge_keyed_avg(List<Pair<Long, Long>> avg) {
    return merge_avg(avg);
  }

  @ClojureClass(className = "backtype.storm.stats#merge-avg")
  public static Pair<Long, Long> merge_avg(List<Pair<Long, Long>> avg) {
    Pair<Long, Long> rtn = new Pair<Long, Long>(0l, 0l);
    for (Pair<Long, Long> p : avg) {
      rtn.setFirst(rtn.getFirst() + p.getFirst());
      rtn.setSecond(rtn.getSecond() + p.getSecond());
    }
    return rtn;
  }

}
