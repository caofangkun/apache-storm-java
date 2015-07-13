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
package org.apache.storm.daemon.builtinMetrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.ClojureClass;

import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.MultiReducedMetric;

@ClojureClass(className = "backtype.storm.daemon.builtin-metrics#BuiltinSpoutMetrics")
public class BuiltinSpoutMetrics {
  private Map<String, IMetric> spoutMetrics = new HashMap<String, IMetric>();
  private MultiCountMetric ackCount;
  private MultiCountMetric failCount;
  private MultiCountMetric emitCount;
  private MultiCountMetric transferCount;
  private MultiReducedMetric completeLatency;

  public BuiltinSpoutMetrics() {
    this.ackCount = new MultiCountMetric();
    this.failCount = new MultiCountMetric();
    this.emitCount = new MultiCountMetric();
    this.transferCount = new MultiCountMetric();
    this.completeLatency = new MultiReducedMetric(new MeanReducer());

    spoutMetrics.put("ackCount", ackCount);
    spoutMetrics.put("failCount", failCount);
    spoutMetrics.put("emitCount", emitCount);
    spoutMetrics.put("transferCount", transferCount);
    spoutMetrics.put("completeLatency", completeLatency);
  }

  public MultiReducedMetric getCompleteLatency() {
    return completeLatency;
  }

  public void setCompleteLatency(MultiReducedMetric completeLatency) {
    this.completeLatency = completeLatency;
  }

  public Map<String, IMetric> getSpoutMetrics() {
    return spoutMetrics;
  }

  public void setSpoutMetrics(Map<String, IMetric> spoutMetrics) {
    this.spoutMetrics = spoutMetrics;
  }

  public MultiCountMetric getAckCount() {
    return ackCount;
  }

  public void setAckCount(MultiCountMetric ackCount) {
    this.ackCount = ackCount;
  }

  public MultiCountMetric getFailCount() {
    return failCount;
  }

  public void setFailCount(MultiCountMetric failCount) {
    this.failCount = failCount;
  }

  public MultiCountMetric getEmitCount() {
    return emitCount;
  }

  public void setEmitCount(MultiCountMetric emitCount) {
    this.emitCount = emitCount;
  }

  public MultiCountMetric getTransferCount() {
    return transferCount;
  }

  public void setTransferCount(MultiCountMetric transferCount) {
    this.transferCount = transferCount;
  }
}
