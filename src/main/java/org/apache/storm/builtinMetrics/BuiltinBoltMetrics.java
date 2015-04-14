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
package org.apache.storm.builtinMetrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.ClojureClass;

import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.MultiReducedMetric;

@ClojureClass(className = "backtype.storm.daemon.builtin-metrics#BuiltinBoltMetrics")
public class BuiltinBoltMetrics {
  private Map<String, IMetric> boltMetrics = new HashMap<String, IMetric>();
  private MultiCountMetric ackCount;
  private MultiCountMetric failCount;
  private MultiCountMetric emitCount;
  private MultiCountMetric transferCount;
  private MultiReducedMetric processLatency;
  private MultiCountMetric executeCount;
  private MultiReducedMetric executeLatency;

  public BuiltinBoltMetrics() {
    this.ackCount = new MultiCountMetric();
    this.failCount = new MultiCountMetric();
    this.emitCount = new MultiCountMetric();
    this.transferCount = new MultiCountMetric();

    this.processLatency = new MultiReducedMetric(new MeanReducer());
    this.executeCount = new MultiCountMetric();
    this.executeLatency = new MultiReducedMetric(new MeanReducer());

    boltMetrics.put("ackCount", ackCount);
    boltMetrics.put("failCount", failCount);
    boltMetrics.put("emitCount", emitCount);
    boltMetrics.put("transferCount", transferCount);
    boltMetrics.put("processLatency", processLatency);
    boltMetrics.put("executeCount", executeCount);
    boltMetrics.put("executeLatency", executeLatency);
  }

  public MultiReducedMetric getProcessLatency() {
    return processLatency;
  }

  public void setProcessLatency(MultiReducedMetric processLatency) {
    this.processLatency = processLatency;
  }

  public MultiCountMetric getExecuteCount() {
    return executeCount;
  }

  public void setExecuteCount(MultiCountMetric executeCount) {
    this.executeCount = executeCount;
  }

  public MultiReducedMetric getExecuteLatency() {
    return executeLatency;
  }

  public void setExecuteLatency(MultiReducedMetric executeLatency) {
    this.executeLatency = executeLatency;
  }

  public Map<String, IMetric> getBoltMetrics() {
    return boltMetrics;
  }

  public void setBoltMetrics(Map<String, IMetric> boltMetrics) {
    this.boltMetrics = boltMetrics;
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
