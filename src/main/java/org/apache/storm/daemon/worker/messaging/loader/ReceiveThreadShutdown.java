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

import java.net.Socket;
import java.util.List;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.AsyncLoopThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.daemon.Shutdownable;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;

@ClojureClass(className = "backtype.storm.messaging.loader#launch-receive-thread!#fn")
public class ReceiveThreadShutdown implements Shutdownable {
  private static final Logger LOG = LoggerFactory
      .getLogger(ReceiveThreadShutdown.class);

  protected String topologyId;
  protected IContext context;
  protected Socket kill_socket;
  protected List<AsyncLoopThread> vthreads;
  protected int port;
  protected WorkerData workerData;

  public ReceiveThreadShutdown(WorkerData workerData, String topologyId,
      IContext context, List<AsyncLoopThread> vthreads, int port) {
    this.workerData = workerData;
    this.topologyId = topologyId;
    this.context = context;
    this.vthreads = vthreads;
    this.port = port;
  }

  @Override
  public void shutdown() {
    workerData.getStormActiveAtom().set(false);
    try {
      String localHostname = CoreUtil.localHostname();
      IConnection killSocket = context.connect(topologyId, localHostname, port);
      LOG.info("Shutting down receiving-thread: [{}, {}]", topologyId, port);
      byte[] bytearray = {};
      killSocket.send(-1, bytearray);
      killSocket.close();

      LOG.info("Waiting for receiving-thread: [{}, {}] to die", topologyId,
          port);
      for (AsyncLoopThread vthread : vthreads) {
        vthread.stop = true;
        vthread.join();
      }
    } catch (Exception e) {
      LOG.error("Shutting down receiving-thread: [{}, {}] error! For "
          + CoreUtil.stringifyError(e), topologyId, port);
    }
    LOG.info("Shutdown receiving-thread:[{}, {}] ", topologyId, port);
  }
}
