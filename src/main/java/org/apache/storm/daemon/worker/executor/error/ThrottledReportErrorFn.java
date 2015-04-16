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
package org.apache.storm.daemon.worker.executor.error;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.daemon.worker.executor.ExecutorData;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.Time;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#throttled-report-error-fn")
public class ThrottledReportErrorFn implements ITaskReportErr {
  private static Logger LOG = LoggerFactory
      .getLogger(ThrottledReportErrorFn.class);
  private StormClusterState stormClusterState;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private String stormId;
  private String componentId;
  private int errorIntervalSecs;
  private int maxPerInterval;
  private AtomicInteger intervalErrors = new AtomicInteger(0);
  private String host = "unknown";
  private int port;

  public ThrottledReportErrorFn(ExecutorData executorData) {
    this.stormClusterState = executorData.getStormClusterState();

    this.stormConf = executorData.getStormConf();
    this.errorIntervalSecs =
        CoreUtil.parseInt(
            stormConf.get(Config.TOPOLOGY_ERROR_THROTTLE_INTERVAL_SECS), 10);
    this.maxPerInterval =
        CoreUtil.parseInt(
            stormConf.get(Config.TOPOLOGY_MAX_ERROR_REPORT_PER_INTERVAL), 5);
    this.stormId = executorData.getStormId();
    this.componentId = executorData.getComponentId();
    this.host = executorData.getHostName();
    this.port = executorData.getWorkerContext().getThisWorkerPort();
  }

  @Override
  public void report(Throwable error) {
    String stormZookeeperRoot =
        CoreUtil.parseString(stormConf.get(Config.STORM_ZOOKEEPER_ROOT),
            "/jstorm");
    LOG.error("Report error to " + stormZookeeperRoot + "/errors/" + stormId
        + "/" + componentId + "\n", error);
    int intervalStartTime = Time.currentTimeSecs();

    if (CoreUtil.timeDelta(intervalStartTime) > errorIntervalSecs) {
      intervalErrors.set(0);
      intervalStartTime = Time.currentTimeSecs();
    }

    if (intervalErrors.incrementAndGet() <= maxPerInterval) {
      try {
        stormClusterState.reportError(stormId, componentId, host, port, error);
      } catch (Exception e) {
        LOG.error("Failed update error to " + stormZookeeperRoot + "/errors/"
            + stormId + "/" + componentId + "\n", e);
      }
    }
  }
}
