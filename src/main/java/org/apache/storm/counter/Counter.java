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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.util.IntervalCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class Counter {
  private static Logger LOG = LoggerFactory.getLogger(Counter.class);

  private String name;
  private String displayName;
  private long value = 0;
  private IntervalCheck intervalCheck = new IntervalCheck();

  protected Counter() {
  }

  protected Counter(String name, String displayName) {
    this.name = name;
    this.displayName = displayName;
    intervalCheck.setInterval(60);
  }

  public Counter(String name, String displayName, long value) {
    this.name = name;
    this.displayName = displayName;
    this.value = value;
    intervalCheck.setInterval(60);
  }

  public synchronized String getName() {
    return name;
  }

  public synchronized String getDisplayName() {
    return displayName;
  }

  public synchronized long getValue() {
    return value;
  }

  public synchronized void increment(long incr) {
    value += incr;
  }

  public synchronized void update(long amount) {
    value = amount;
  }

  private AtomicLong times = new AtomicLong(0);

  public synchronized void tps() {
    long timesValue = times.incrementAndGet();
    Long pass = intervalCheck.checkAndGet();
    if (pass != null) {
      times.set(0);
      value = (timesValue / pass);
      LOG.info(displayName + ":" + value);
    }
  }

  public synchronized void tps(String info) {
    long timesValue = times.incrementAndGet();
    Long pass = intervalCheck.checkAndGet();
    if (pass != null) {
      times.set(0);
      value = (timesValue / pass);
      LOG.info(info + "-" + displayName + ":" + value + ",total:" + timesValue);
    }
  }

  public synchronized boolean equals(Object genericRight) {
    if (genericRight instanceof Counter) {
      synchronized (genericRight) {
        Counter right = (Counter) genericRight;
        return name.equals(right.name) && displayName.equals(right.displayName)
            && value == right.value;
      }
    }
    return false;
  }

  public synchronized int hashCode() {
    return name.hashCode() + displayName.hashCode();
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public void setValue(long value) {
    this.value = value;
  }

}
