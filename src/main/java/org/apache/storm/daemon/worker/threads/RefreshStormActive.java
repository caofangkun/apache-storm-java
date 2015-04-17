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
package org.apache.storm.daemon.worker.threads;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.daemon.nimbus.transitions.StatusType;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.StormBase;
import backtype.storm.generated.TopologyStatus;

@ClojureClass(className = "backtype.storm.daemon.worker#refresh-storm-active")
public class RefreshStormActive extends RunnableCallback {
  private static final long serialVersionUID = 1L;

  private static Logger LOG = LoggerFactory.getLogger(RefreshStormActive.class);

  private WorkerData workerData;

  private StormClusterState stormClusterState;
  private String topologyId;
  private int frequence;
  private Exception exception = null;
  private AtomicBoolean active;

  public RefreshStormActive(WorkerData workerData, RunnableCallback callback,
      @SuppressWarnings("rawtypes") Map conf) {
    this.workerData = workerData;
    this.stormClusterState = workerData.getStormClusterState();
    this.topologyId = workerData.getTopologyId();
    this.active = workerData.getStormActiveAtom();
    this.frequence =
        CoreUtil.parseInt(conf.get(Config.TASK_REFRESH_POLL_SECS), 10);
  }

  public RefreshStormActive(WorkerData workerData,
      @SuppressWarnings("rawtypes") Map conf) {
    this(workerData, null, conf);
  }

  @Override
  public void run() {

    try {
      TopologyStatus newTopologyStatus = TopologyStatus.ACTIVE;
      StormBase base = stormClusterState.stormBase(topologyId, this);
      if (base == null) {
        LOG.warn("Failed to get StromBase from ZK of " + topologyId
            + " and change statue to killed");
        newTopologyStatus = TopologyStatus.KILLED;
      } else {
        newTopologyStatus = base.get_status();
      }
      TopologyStatus oldTopologyStatus = workerData.getTopologyStatus();

      if (newTopologyStatus.equals(oldTopologyStatus)) {
        return;
      }

      LOG.info("Old TopologyStatus:" + oldTopologyStatus
          + ", new TopologyStatus:" + newTopologyStatus);
      workerData.setTopologyStatus(newTopologyStatus);
      boolean isActive = newTopologyStatus.equals(StatusType.active);
      workerData.getStormActiveAtom().set(isActive);
    } catch (Exception e) {
      LOG.error("Failed to get topology from ZK ", CoreUtil.stringifyError(e));
    }
  }

  @Override
  public Exception error() {
    if (null != exception) {
      LOG.error("Refresh Storm Active exception: {}",
          CoreUtil.stringifyError(exception));
    }
    return exception;
  }

  @Override
  public Object getResult() {
    if (this.active.get()) {
      return frequence;
    }
    return -1;
  }
}
