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
package org.apache.storm.daemon.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.ClusterState;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.daemon.worker.executor.ExecutorShutdown;
import org.apache.storm.daemon.worker.executor.ShutdownableDameon;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.AsyncLoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.WorkerTopologyContext;

@ClojureClass(className = "backtype.storm.daemon.worker#mk-worker#ret")
public class WorkerShutdown implements ShutdownableDameon {
  private static Logger LOG = LoggerFactory.getLogger(WorkerShutdown.class);
  private List<ExecutorShutdown> executors;
  private HashMap<WorkerSlot, IConnection> cachedNodeportSocket;
  private Shutdownable workerReceiveThreadShutdown;
  private IContext context;
  private AsyncLoopThread[] threads;
  private StormClusterState stormClusterState;
  private ClusterState clusterState;
  private WorkerData workerData;
  private String stormId;
  private String assignmentId;
  private int port;

  public WorkerShutdown(WorkerData workerData,
      List<ExecutorShutdown> executors,
      Shutdownable workerReceiveThreadShutdown, AsyncLoopThread[] threads) {
    this.workerData = workerData;
    this.stormId = workerData.getTopologyId();
    this.assignmentId = workerData.getAssignmentId();
    this.port = workerData.getPort();
    this.executors = executors;
    this.workerReceiveThreadShutdown = workerReceiveThreadShutdown;
    this.threads = threads;
    this.cachedNodeportSocket = workerData.getCachedNodeportToSocket();
    this.context = workerData.getContext();
    this.stormClusterState = workerData.getStormClusterState();
    this.clusterState = workerData.getZkClusterstate();
    Runtime.getRuntime().addShutdownHook(new Thread(this));
  }

  @Override
  public void shutdown() {
    workerData.getStormActiveAtom().set(false);

    LOG.info("Shutting down worker " + workerData.getTopologyId() + " "
        + workerData.getAssignmentId() + " " + workerData.getPort());

    for (IConnection socket : cachedNodeportSocket.values()) {
      // this will do best effort flushing since the linger period
      // was set on creation
      socket.close();
    }

    LOG.info("Shutting down receive thread");
    workerReceiveThreadShutdown.shutdown();
    LOG.info("Shut down receive thread");

    LOG.info("Terminating messaging context");
    LOG.info("Shutting down executors");
    for (ShutdownableDameon executor : executors) {
      executor.shutdown();
    }
    LOG.info("Shut down executors");

    // this is fine because the only time this is shared is when it's a local
    // context,in which case it's a noop
    context.term();
    LOG.info("Shutting down transfer thread");
    workerData.getTransferQueue().haltWithInterrupt();

    for (AsyncLoopThread thread : threads) {
      try {
        thread.interrupt();
        thread.join();
        LOG.info("Shut down transfer thread");
      } catch (InterruptedException e) {
        LOG.error("join thread", e);
      }
    }

    closeResources(workerData);

    // TODO: here need to invoke the "shutdown" method of WorkerHook

    if (!clusterState.isClosed()) {
      try {
        stormClusterState.removeWorkerHeartbeat(stormId, assignmentId, port);
        LOG.info("Removed worker heartbeat stormId:{}, port:{}", assignmentId,
            port);
        LOG.info("Disconnecting from storm cluster state context");
        stormClusterState.disconnect();
        clusterState.close();
      } catch (Exception e) {
        LOG.info("Shutdown error {} ", CoreUtil.stringifyError(e));
      }
    }

    LOG.info("Shut down worker " + stormId + " " + assignmentId + " " + port);
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#close-resources")
  private void closeResources(WorkerData workerData) {
    Map<String, Object> dr = workerData.getDefaultSharedResources();
    LOG.info("Shutting down default resources");
    ExecutorService es =
        (ExecutorService) dr.get(WorkerTopologyContext.SHARED_EXECUTOR);
    es.shutdownNow();
    LOG.info("Shut down default resources");
  }

  public void join() throws InterruptedException {
    for (ExecutorShutdown task : executors) {
      task.join();
    }
    for (AsyncLoopThread thread : threads) {
      thread.join();
    }
  }

  @Override
  public boolean waiting() {
    Boolean isExistsWait = false;
    for (ShutdownableDameon task : executors) {
      if (task.waiting()) {
        isExistsWait = true;
        break;
      }
    }
    for (AsyncLoopThread thread : threads) {
      if (thread.isSleeping()) {
        isExistsWait = true;
      }
    }

    return isExistsWait;
  }

  @Override
  public void run() {
    shutdown();
  }
}
