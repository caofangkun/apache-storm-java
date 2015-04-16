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
package org.apache.storm.daemon.supervisor.heartbeat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.daemon.common.SupervisorInfo;
import org.apache.storm.daemon.supervisor.SupervisorData;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.utils.MutableLong;

@ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor#heartbeat-fn")
public class SupervisorHeartbeat extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory
      .getLogger(SupervisorHeartbeat.class);

  @SuppressWarnings("rawtypes")
  private Map conf;
  private StormClusterState stormClusterState;
  private String supervisorId;
  private SupervisorData supervisorData;
  private ISupervisor isupervisor;
  private MutableLong retryCount;
  private int frequence;
  private Exception exception;

  @SuppressWarnings("rawtypes")
  public SupervisorHeartbeat(Map conf, SupervisorData supervisorData,
      ISupervisor isupervisor) throws IOException {
    this.supervisorData = supervisorData;
    this.isupervisor = isupervisor;
    this.stormClusterState = supervisorData.getStormClusterState();
    this.supervisorId = supervisorData.getSupervisorId();
    this.conf = conf;
    this.retryCount = new MutableLong(0);
    this.frequence =
        CoreUtil.parseInt(
            conf.get(Config.SUPERVISOR_HEARTBEAT_FREQUENCY_SECS), 10);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void run() {

    if (!supervisorData.getActive().get()) {
      return;
    }

    try {
      HashSet<Integer> workPorts = new HashSet<Integer>();
      workPorts.addAll(supervisorData.getCurrAssignment().keySet());
      SupervisorInfo supervisorInfo =
          new SupervisorInfo(CoreUtil.current_time_secs(),
              supervisorData.getMyHostname(), supervisorData.getAssignmentId(),
              workPorts, isupervisor.getMetadata(),
              (Map) conf.get(Config.SUPERVISOR_SCHEDULER_META), supervisorData
                  .getUptime().uptime(), conf);
      LOG.debug("supervisor heartbeat: {} : {} ", supervisorId,
          supervisorInfo.toString());
      stormClusterState.supervisorHeartbeat(supervisorId, supervisorInfo);
      retryCount = new MutableLong(0);
    } catch (Exception e) {
      LOG.error("Failed to update SupervisorInfo to ZK", e);
      retryCount.increment();
      if (retryCount.get() >= 3) {
        exception = new RuntimeException(e);
      }
    }
  }

  @Override
  public Exception error() {
    if (null != exception) {
      LOG.error("Supervisor Heartbeat exception: {}",
          CoreUtil.stringifyError(exception));
    }
    return exception;
  }

  @Override
  public Object getResult() {
    if (supervisorData.getActive().get()) {
      return frequence;
    }
    return -1;
  }
}