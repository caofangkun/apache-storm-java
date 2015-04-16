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
package org.apache.storm.daemon.supervisor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.ClojureClass;
import backtype.storm.daemon.Shutdownable;

import com.tencent.jstorm.cluster.StormClusterState;
import com.tencent.jstorm.daemon.common.DaemonCommon;
import com.tencent.jstorm.event.EventManager;
import com.tencent.jstorm.http.HttpServer;
import com.tencent.jstorm.utils.ServerUtils;
import com.tencent.jstorm.utils.thread.AsyncLoopThread;

@ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor")
public class SupervisorManager extends ShutdownWork implements Shutdownable,
    SupervisorDaemon, DaemonCommon, Runnable {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(SupervisorManager.class);

  @SuppressWarnings("rawtypes")
  private Map conf;
  private String supervisorId;
  private AtomicBoolean active;
  private EventManager processesEventManager;
  private EventManager eventManager;
  private StormClusterState stormClusterState;
  private ConcurrentHashMap<String, String> workerThreadPidsAtom;
  private volatile boolean isFinishShutdown = false;
  private HttpServer httpServer;
  private SupervisorData supervisorData;
  private AsyncLoopThread[] threads;

  public SupervisorManager(SupervisorData supervisorData,
      EventManager processesEventManager, EventManager eventManager,
      HttpServer httpServer, AsyncLoopThread[] threads) {
    this.supervisorData = supervisorData;
    this.conf = supervisorData.getConf();
    this.supervisorId = supervisorData.getSupervisorId();
    this.active = supervisorData.getActive();
    this.processesEventManager = processesEventManager;
    this.eventManager = eventManager;
    this.stormClusterState = supervisorData.getStormClusterState();
    this.workerThreadPidsAtom = supervisorData.getWorkerThreadPids();
    this.httpServer = httpServer;
    this.threads = threads;

    Runtime.getRuntime().addShutdownHook(new Thread(this));
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor#shutdown")
  public void shutdown() {
    LOG.info("Shutting down supervisor {}", supervisorId);

    active.set(false);

    if (httpServer != null) {
      try {
        httpServer.stop();
        httpServer = null;
        LOG.info("Shutting down LogViewer Server ");
      } catch (Exception e) {
        LOG.warn("Failed to shut down LogViewer Server for "
            + ServerUtils.stringifyError(e));
      }
    }

    eventManager.shutdown();
    processesEventManager.shutdown();

    for (AsyncLoopThread thread : threads) {
      try {
        thread.interrupt();
        thread.join();
        LOG.info("Shut down transfer thread");
      } catch (InterruptedException e) {
        LOG.error("join thread", e);
      }
    }

    try {
      stormClusterState.disconnect();
      LOG.info("Disconnect Storm Cluster State ");
    } catch (Exception e) {
      LOG.error("Failed to shutdown ZK client", e);
    }
    isFinishShutdown = true;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor#shutdown-all-workers")
  public void shutdownAllWorkers() {
    LOG.info("Begin to shutdown all workers");
    Set<String> myWorkerIds = SupervisorUtils.myWorkerIds(conf);

    for (String workerId : myWorkerIds) {
      try {
        shutdownWorker(supervisorData, workerId);
      } catch (Exception e) {
        String errMsg =
            "Failed to shutdown supervisorId:" + supervisorId + ",workerId:"
                + workerId + "workerThreadPidsAtom:" + workerThreadPidsAtom
                + "\n";
        LOG.error(errMsg, e);

      }
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  @ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor#get-conf")
  public Map getConf() {
    return conf;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor#get-id")
  public String getId() {
    return supervisorId;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor#waiting?")
  public boolean waiting() {
    if (!active.get()) {
      return true;
    }

    if (eventManager.waiting() && processesEventManager.waiting()) {
      return false;
    }
    return true;
  }

  public void run() {
    shutdown();
  }

  public boolean isFinishShutdown() {
    return isFinishShutdown;
  }
}
