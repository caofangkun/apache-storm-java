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
package org.apache.storm.daemon.worker.stats.rolling;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.stats#RollingWindowSet")
public class RollingWindowSet implements Serializable {
  private static final long serialVersionUID = 1L;
  private RunnableCallback updater;
  private RunnableCallback extractor;
  private volatile RollingWindow[] windows;
  // all_time store the all_time result
  private Object allTime;

  public RollingWindowSet(RunnableCallback updater, RunnableCallback extractor,
      RollingWindow[] windows, Object allTime) {
    this.updater = updater;
    this.extractor = extractor;
    this.windows = windows;
    this.allTime = allTime;
  }

  public RunnableCallback getUpdater() {
    return updater;
  }

  public void setUpdater(RunnableCallback updater) {
    this.updater = updater;
  }

  public RunnableCallback getExtractor() {
    return extractor;
  }

  public void setExtractor(RunnableCallback extractor) {
    this.extractor = extractor;
  }

  public RollingWindow[] getWindows() {
    return windows;
  }

  public void setWindows(RollingWindow[] windows) {
    this.windows = windows;
  }

  public Object getAllTime() {
    return allTime;
  }

  public void setAllTime(Object all_time) {
    this.allTime = all_time;
  }

  @ClojureClass(className = "backtype.storm.stats#rolling-window-set")
  public static RollingWindowSet rolling_window_set(RunnableCallback updater,
      RunnableCallback merger, RunnableCallback extractor, Integer numBuckets,
      Integer[] bucketSizes) {
    RollingWindow[] windows = new RollingWindow[bucketSizes.length];
    int bSize = bucketSizes.length;
    for (int i = 0; i < bSize; i++) {
      windows[i] =
          RollingWindow.rolling_window(updater, merger, extractor,
              bucketSizes[i], numBuckets);
    }
    return new RollingWindowSet(updater, extractor, windows, null);
  }

  @ClojureClass(className = "backtype.storm.stats#update-rolling-window-set ")
  public void update_rolling_window_set(Object[] args) {
    synchronized (this) {
      int now = CoreUtil.current_time_secs();
      int windowsLength = windows.length;
      for (int i = 0; i < windowsLength; i++) {
        windows[i] = windows[i].update_rolling_window(now, args);
      }

      UpdateParams p = new UpdateParams();
      p.setArgs(args);
      p.setCurr(getAllTime());

      setAllTime(updater.execute(p));
    }
  }

  @ClojureClass(className = "backtype.storm.stats#cleanup-rolling-window-set")
  public RollingWindowSet cleanup_rolling_window_set() {
    synchronized (this) {
      for (int i = 0; i < windows.length; i++) {
        windows[i] = windows[i].cleanup_rolling_window();
      }
      return this;
    }
  }

  @ClojureClass(className = "backtype.storm.stats#value-rolling-window-set")
  public HashMap<Integer, Object> value_rolling_window_set() {

    HashMap<Integer, Object> rtn = new HashMap<Integer, Object>();

    synchronized (this) {
      int windowsLength = windows.length;
      for (int i = 0; i < windowsLength; i++) {
        rtn.put(windows[i].rolling_window_size(),
            windows[i].value_rolling_window());
      }

      Object result = extractor.execute(allTime);
      rtn.put(0, result);
    }

    return rtn;
  }
}
