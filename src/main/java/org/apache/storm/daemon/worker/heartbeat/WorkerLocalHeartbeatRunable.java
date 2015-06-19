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
package org.apache.storm.daemon.worker.heartbeat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.ClojureClass;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.localstate.LocalStateUtil;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.MutableLong;

public class WorkerLocalHeartbeatRunable extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory
      .getLogger(WorkerLocalHeartbeatRunable.class);

  private WorkerData workerData;
  private AtomicBoolean active;
  private Map<Object, Object> conf;
  private String worker_id;
  private Integer port;
  private String topologyId;
  private List<ExecutorInfo> executors;
  private String processId;
  private int frequence;
  private Map<Integer, Integer> virtualToRealPort;

  @SuppressWarnings("unchecked")
  public WorkerLocalHeartbeatRunable(WorkerData workerData) {

    this.workerData = workerData;
    this.conf = workerData.getStormConf();
    this.worker_id = workerData.getWorkerId();
    this.port = workerData.getPort();
    this.topologyId = workerData.getTopologyId();
    this.executors =
        new ArrayList<ExecutorInfo>(workerData.getExecutors());
    this.active = workerData.getStormActiveAtom();
    this.processId = workerData.getProcessId();
    this.frequence =
        CoreUtil.parseInt(conf.get(Config.WORKER_HEARTBEAT_FREQUENCY_SECS),
            10);
    this.virtualToRealPort =
        (Map<Integer, Integer>) conf.get("storm.virtual.real.ports");
    if (virtualToRealPort.containsKey(port)) {
      this.port = virtualToRealPort.get(port);
    }  }

  @ClojureClass(className = "backtype.storm.daemon.worker#do-heartbeat")
  public void doHeartbeat() throws IOException {

    int currtime = CoreUtil.current_time_secs();
    WorkerLocalHeartbeat hb =
        new WorkerLocalHeartbeat(currtime, topologyId, executors, port,
            processId);
    LOG.debug("Doing heartbeat " + hb.toString());
    LocalState state = ConfigUtil.workerState(conf, worker_id);
    // do the local-file-system heartbeat.
    LocalStateUtil.lsWorkerHeartbeat(state, currtime, workerData.getTopologyId(), executors, port);
    // this is just in case supervisor is down so that disk doesn't fill up.
    // it shouldn't take supervisor 120 seconds between listing dir
    // and reading it
    state.cleanup(8);
  }

  @Override
  public void run() {
    try {
      doHeartbeat();
    } catch (IOException e) {
      LOG.error("Failed doing Worker HeartBeat ", CoreUtil.stringifyError(e));
    }
  }

  @Override
  public Exception error() {
    return null;
  }

  @Override
  public Object getResult() {
    if (this.active.get()) {
      return frequence;
    }
    return -1;
  }

  public WorkerData getWorkerData() {
    return workerData;
  }

  public void setWorkerData(WorkerData workerData) {
    this.workerData = workerData;
  }
}
