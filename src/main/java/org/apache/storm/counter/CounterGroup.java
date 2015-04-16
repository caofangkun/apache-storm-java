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

import java.util.Iterator;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TreeMap;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class CounterGroup implements Iterable<Counter> {

  private String name;
  private String displayName;
  private TreeMap<String, Counter> counters = new TreeMap<String, Counter>();
  private ResourceBundle bundle = null;

  private static ResourceBundle getResourceBundle(String enumClassName) {
    String bundleName = enumClassName.replace('$', '_');
    return ResourceBundle.getBundle(bundleName);
  }

  protected CounterGroup(String name) {
    this.name = name;
    try {
      bundle = getResourceBundle(name);
    } catch (MissingResourceException neverMind) {
    }
    displayName = localize("CounterGroupName", name);
  }

  public CounterGroup(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized String getDisplayName() {
    return displayName;
  }

  public synchronized void addCounter(Counter counter) {
    counters.put(counter.getName(), counter);
  }

  public Counter findCounter(String counterName, String displayName) {
    Counter result = counters.get(counterName);
    if (result == null) {
      result = new Counter(counterName, displayName);
      counters.put(counterName, result);
    }
    return result;
  }

  public synchronized Counter findCounter(String counterName) {
    Counter result = counters.get(counterName);
    if (result == null) {
      String displayName = localize(counterName, counterName);
      result = new Counter(counterName, displayName);
      counters.put(counterName, result);
    }
    return result;
  }

  @Override
  public Iterator<Counter> iterator() {
    return counters.values().iterator();
  }

  private String localize(String key, String defaultValue) {
    String result = defaultValue;
    if (bundle != null) {
      try {
        result = bundle.getString(key);
      } catch (MissingResourceException mre) {
      }
    }
    return result;
  }

  public synchronized int size() {
    return counters.size();
  }

  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof CounterGroup) {
      Iterator<Counter> right =
          ((CounterGroup) genericRight).counters.values().iterator();
      Iterator<Counter> left = counters.values().iterator();
      while (left.hasNext()) {
        if (!right.hasNext() || !left.next().equals(right.next())) {
          return false;
        }
      }
      return !right.hasNext();
    }
    return false;
  }

  public synchronized int hashCode() {
    return counters.hashCode();
  }

  public synchronized void incrAllCounters(CounterGroup rightGroup) {
    for (Counter right : rightGroup.counters.values()) {
      Counter left = findCounter(right.getName(), right.getDisplayName());
      left.increment(right.getValue());
    }
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

}
