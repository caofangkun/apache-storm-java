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
package org.apache.storm.daemon.worker.messaging.loader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.SystemExitKillFn;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.transfer.TransferLocalFn;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.AsyncLoopThread;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.daemon.Shutdownable;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

public class WorkerReceiveThreadShutdown {

  private final static Logger LOG = LoggerFactory
      .getLogger(WorkerReceiveThreadShutdown.class);

  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private String supervisorId;
  private Integer port;
  private IContext context;
  private String stormId;
  private IConnection socket;
  private WorkerData workerData;

  public WorkerReceiveThreadShutdown(WorkerData workerData) {
    this.workerData = workerData;
    this.stormConf = workerData.getStormConf();
    this.supervisorId = workerData.getAssignmentId();
    this.port = workerData.getPort();
    this.context = workerData.getContext();
    this.stormId = workerData.getTopologyId();
    this.socket = workerData.getReceiver();
  }

  @ClojureClass(className = "backtype.storm.messaging.loader#launch-receive-thread!")
  public Shutdownable launchReceiveThread() throws InterruptedException {
    LOG.info("Launching receive-thread for {}:{}", supervisorId, port);

    List<AsyncLoopThread> vthreads =
        mkReceiveThreads(
            workerData,
            stormId,
            port,
            socket,
            workerData.getTransferLocalFn(),
            CoreUtil.parseInt(
                stormConf.get(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE), 8),
            workerData.getReceiverThreadCount());
    return new ReceiveThreadShutdown(workerData, stormId, context, vthreads,
        port);

  }

  @ClojureClass(className = "backtype.storm.messaging.loader#mk-receive-threads")
  private List<AsyncLoopThread> mkReceiveThreads(WorkerData workerData,
      String stormId, int port, IConnection socket,
      TransferLocalFn transferLocalFn, int maxBufferSize, Integer threadCount) {
    List<AsyncLoopThread> ret = new ArrayList<AsyncLoopThread>();
    for (int threadId = 0; threadId < threadCount; threadId++) {
      RunnableCallback receiveThread =
          new ReceiveThreadCallback(workerData, stormId, port, socket,
              transferLocalFn, maxBufferSize, threadId);
      String threadName = "worker-receiver-thread-" + threadId;
      AsyncLoopThread vthread =
          new AsyncLoopThread(receiveThread, true, new SystemExitKillFn(
              "worker-receive-thread exits"), Thread.NORM_PRIORITY, true,
              threadName);
      ret.add(vthread);
    }
    return ret;
  }

}
