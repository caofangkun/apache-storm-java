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
import java.util.Map.Entry;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.stats.Pair;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.stats#extract-keyed-avg")
public class KeyedAvgExtractor extends RunnableCallback {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    Map<Object, Double> result = null;
    if (args != null && args.length > 0) {
      Map<Object, Pair<Long, Long>> v = (Map<Object, Pair<Long, Long>>) args[0];
      result = extract_keyed_avg(v);
    }

    if (result == null) {
      result = new HashMap<Object, Double>();
    }

    return result;
  }

  @ClojureClass(className = "backtype.storm.stats#extract-keyed-avg")
  private static Map<Object, Double> extract_keyed_avg(
      Map<Object, Pair<Long, Long>> map) {
    Map<Object, Double> rtn = new HashMap<Object, Double>();
    if (map != null) {
      for (Entry<Object, Pair<Long, Long>> e : map.entrySet()) {
        rtn.put(e.getKey(), extract_avg(e.getValue()));
      }
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.stats#extract-avg")
  private static double extract_avg(Pair<Long, Long> pair) {
    if (pair.getSecond() == 0) {
      return 0d;
    }
    return (pair.getFirst() * 1.0) / pair.getSecond();
  }
}
