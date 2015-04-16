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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.stats#RollingWindow")
public class RollingWindow {

  private RunnableCallback updater;
  private RunnableCallback merger;
  private RunnableCallback extractor;
  private Integer bucketSizeSecs;
  private Integer numBuckets;
  // <TimeSecond, Map<streamId, staticsValue>>
  @SuppressWarnings("rawtypes")
  private volatile Map<Integer, Map> buckets;

  @SuppressWarnings("rawtypes")
  public RollingWindow(RunnableCallback updater, RunnableCallback merger,
      RunnableCallback extractor, Integer bucketSizeSecs, Integer numBuckets,
      Map<Integer, Map> buckets) {
    this.updater = updater;
    this.merger = merger;
    this.extractor = extractor;
    this.bucketSizeSecs = bucketSizeSecs;
    this.numBuckets = numBuckets;
    this.buckets = buckets;
  }

  public RunnableCallback getUpdater() {
    return updater;
  }

  public void setUpdater(RunnableCallback updater) {
    this.updater = updater;
  }

  public RunnableCallback getMerger() {
    return merger;
  }

  public void setMerger(RunnableCallback merger) {
    this.merger = merger;
  }

  public RunnableCallback getExtractor() {
    return extractor;
  }

  public void setExtractor(RunnableCallback extractor) {
    this.extractor = extractor;
  }

  public Integer getBucketSizeSecs() {
    return bucketSizeSecs;
  }

  public void setBucketSizeSecs(Integer bucketSizeSecs) {
    this.bucketSizeSecs = bucketSizeSecs;
  }

  public Integer getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(Integer numBuckets) {
    this.numBuckets = numBuckets;
  }

  @SuppressWarnings("rawtypes")
  public Map<Integer, Map> getBuckets() {
    return buckets;
  }

  @SuppressWarnings("rawtypes")
  public void setBuckets(Map<Integer, Map> buckets) {
    this.buckets = buckets;
  }

  @ClojureClass(className = "backtype.storm.stats#curr-time-bucket")
  public Integer curr_time_bucket(Integer time_secs) {
    return (Integer) (bucketSizeSecs * (time_secs / bucketSizeSecs));
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.stats#rolling-window")
  public static RollingWindow rolling_window(RunnableCallback updater,
      RunnableCallback merger, RunnableCallback extractor,
      Integer bucketSizeSecs, Integer numBuckets) {

    return new RollingWindow(updater, merger, extractor, bucketSizeSecs,
        numBuckets, new HashMap<Integer, Map>());
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.stats#update-rolling-window")
  public RollingWindow update_rolling_window(Integer time_secs, Object[] args) {
    synchronized (this) {
      // this is 2.5x faster than using update-in...
      Integer time_bucket = curr_time_bucket(time_secs);
      Map curr = buckets.get(time_bucket);

      UpdateParams p = new UpdateParams();
      p.setArgs(args);
      p.setCurr(curr);
      curr = (Map) updater.execute((Object) p);

      buckets.put(time_bucket, curr);

      return this;
    }
  }

  /**
   * 
   * @return <streamId, counter/statics>
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "backtype.storm.stats#value-rolling-window")
  public Object value_rolling_window() {
    synchronized (this) {
      // <TimeSecond, Map<streamId, staticsValue>> buckets
      List<Map> values = new ArrayList<Map>();
      for (Map value : buckets.values()) {
        values.add(value);
      }

      // <streamId, counter> -- result
      Object result = merger.execute(values);
      return extractor.execute(result);
    }

  }

  @ClojureClass(className = "backtype.storm.stats#cleanup-rolling-window")
  public RollingWindow cleanup_rolling_window() {
    int cutoff = CoreUtil.current_time_secs() - rolling_window_size();
    synchronized (this) {
      List<Integer> toremove = new ArrayList<Integer>();
      for (Integer key : buckets.keySet()) {
        if (key < cutoff) {
          toremove.add(key);
        }
      }

      for (Integer i : toremove) {
        buckets.remove(i);
      }
      return this;
    }
  }

  @ClojureClass(className = "backtype.storm.stats#rolling-window-size")
  public int rolling_window_size() {
    return bucketSizeSecs * numBuckets;
  }

}
