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
package org.apache.storm.counter;

import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class Counters implements Iterable<CounterGroup> {

  private static final char GROUP_OPEN = '{';
  private static final char GROUP_CLOSE = '}';
  private static final char COUNTER_OPEN = '[';
  private static final char COUNTER_CLOSE = ']';
  private static final char UNIT_OPEN = '(';
  private static final char UNIT_CLOSE = ')';
  private static char[] charsToEscape = { GROUP_OPEN, GROUP_CLOSE,
      COUNTER_OPEN, COUNTER_CLOSE, UNIT_OPEN, UNIT_CLOSE };

  private Map<Enum<?>, Counter> cache = new IdentityHashMap<Enum<?>, Counter>();

  private TreeMap<String, CounterGroup> groups =
      new TreeMap<String, CounterGroup>();

  public Counters() {
  }

  public void addGroup(CounterGroup group) {
    groups.put(group.getName(), group);
  }

  public Counter findCounter(String groupName, String counterName) {
    CounterGroup grp = getGroup(groupName);
    return grp.findCounter(counterName);
  }

  public synchronized Counter findCounter(Enum<?> key) {
    Counter counter = cache.get(key);
    if (counter == null) {
      counter = findCounter(key.getDeclaringClass().getName(), key.toString());
      cache.put(key, counter);
    }
    return counter;
  }

  public synchronized void incrCounter(Enum key, long amount) {
    findCounter(key).increment(amount);
  }

  public synchronized void incrCounter(String group, String counter, long amount) {
    findCounter(group, counter).increment(amount);
  }

  public synchronized void updateCounter(Enum key, long amount) {
    findCounter(key).update(amount);
  }

  public synchronized void updateCounter(String group, String counter,
      long amount) {
    findCounter(group, counter).update(amount);
  }

  public synchronized void tpsCounter(Enum key) {
    findCounter(key).tps();
  }

  public synchronized void tpsCounter(Enum key, String info) {
    findCounter(key).tps(info);
  }

  public synchronized void tpsCounter(String group, String counter) {
    findCounter(group, counter).tps();
  }

  public synchronized Collection<String> getGroupNames() {
    return groups.keySet();
  }

  public static Counters sum(Counters a, Counters b) {
    Counters counters = new Counters();
    counters.incrAllCounters(a);
    counters.incrAllCounters(b);
    return counters;
  }

  public synchronized int size() {
    int result = 0;
    for (CounterGroup group : this) {
      result += group.size();
    }
    return result;
  }

  public synchronized long getCounter(Enum key) {
    return findCounter(key).getValue();
  }

  @Override
  public Iterator<CounterGroup> iterator() {
    return groups.values().iterator();
  }

  /**
   * Returns the named counter group, or an empty group if there is none with
   * the specified name.
   */
  public synchronized CounterGroup getGroup(String groupName) {
    CounterGroup grp = groups.get(groupName);
    if (grp == null) {
      grp = new CounterGroup(groupName);
      groups.put(groupName, grp);
    }
    return grp;
  }

  /**
   * Returns the total number of counters, by summing the number of counters in
   * each group.
   */
  public synchronized int countCounters() {
    int result = 0;
    for (CounterGroup group : this) {
      result += group.size();
    }
    return result;
  }

  public synchronized String toString() {
    StringBuilder sb = new StringBuilder("Counters: " + countCounters());
    for (CounterGroup group : this) {
      sb.append("\n\t" + group.getDisplayName());
      for (Counter counter : group) {
        sb.append("\n\t\t" + counter.getDisplayName() + "="
            + counter.getValue());
      }
    }
    return sb.toString();
  }

  public void log(Logger log) {
    log.info("Counters: " + size());
    for (CounterGroup group : this) {
      log.info("  " + group.getDisplayName());
      for (Counter counter : group) {
        log.info("    " + counter.getDisplayName() + "=" + counter.getValue());
      }
    }
  }

  public Map<String, Map<String, Long>> countersToMap() {
    // counterName-->(groupname-->amount)
    Map<String, Map<String, Long>> rst =
        new HashMap<String, Map<String, Long>>();
    for (CounterGroup group : this) {
      String groupName = group.getDisplayName();
      for (Counter counter : group) {
        String counterName = counter.getDisplayName();
        Long counterValue = counter.getValue();
        if (rst.containsKey(counterName)) {
          rst.get(counterName).put(groupName, counterValue);
        } else {
          Map<String, Long> groupToAmount = new HashMap<String, Long>();
          groupToAmount.put(groupName, counterValue);
          rst.put(counterName, groupToAmount);
        }
      }
    }
    return rst;
  }

  /**
   * Increments multiple counters by their amounts in another Counters instance.
   * 
   * @param other the other Counters instance
   */
  public synchronized void incrAllCounters(Counters other) {
    for (Map.Entry<String, CounterGroup> rightEntry : other.groups.entrySet()) {
      CounterGroup left = groups.get(rightEntry.getKey());
      CounterGroup right = rightEntry.getValue();
      if (left == null) {
        left = new CounterGroup(right.getName(), right.getDisplayName());
        groups.put(rightEntry.getKey(), left);
      }
      left.incrAllCounters(right);
    }
  }

  public boolean equals(Object genericRight) {
    if (genericRight instanceof Counters) {
      Iterator<CounterGroup> right =
          ((Counters) genericRight).groups.values().iterator();
      Iterator<CounterGroup> left = groups.values().iterator();
      while (left.hasNext()) {
        if (!right.hasNext() || !left.next().equals(right.next())) {
          return false;
        }
      }
      return !right.hasNext();
    }
    return false;
  }

  public int hashCode() {
    return groups.hashCode();
  }

  private static String unescape(String string) {
    return string;
  }

  private static String getBlock(String str, char open, char close,
      Integer index) {

    return str;
  }

}
