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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.tuple.TuplePair;
import org.apache.storm.daemon.worker.transfer.TransferLocalFn;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

@ClojureClass(className = "backtype.storm.messaging.loader#mk-receive-thread")
public class ReceiveThreadCallback extends RunnableCallback {
  private static final long serialVersionUID = 1L;

  private final static Logger LOG = LoggerFactory
      .getLogger(ReceiveThreadCallback.class);

  private String topologyId;
  private IConnection socket;
  private List<TuplePair> batched;
  private TransferLocalFn transferLocalFn;
  private int threadId;
  private int port;
  private AtomicBoolean closed = new AtomicBoolean(false);

  public ReceiveThreadCallback(WorkerData workerData, String topologyId,
      int port, IConnection socket, TransferLocalFn transferLocalFn,
      int maxBufferSize, int threadId) {
    this.topologyId = topologyId;
    this.threadId = threadId;
    this.port = port;
    this.socket = socket;
    this.batched = new ArrayList<TuplePair>();
    this.transferLocalFn = transferLocalFn;
    LOG.info("Starting receive-thread: [stormId: " + topologyId + ", port: "
        + port + ", thread-id: " + threadId + " ]");
  }

  @Override
  public void run() {
    while (!closed.get()) {
      Iterator<TaskMessage> iter = socket.recv(0, threadId);
      if (iter == null) {
        return;
      }
      while (iter.hasNext()) {
        TaskMessage packet = iter.next();
        int task = -1;
        if (packet != null) {
          task = packet.task();
        }
        if (task == -1) {
          LOG.info("Receiving-thread:[" + topologyId + ", " + port
              + "] received shutdown notice");
          socket.close();
          closed.set(true);
        } else {
          batched.add(new TuplePair(task, packet.message()));
        }
      }

      if (batched.size() > 0) {
        transferLocalFn.transfer(batched);
        batched = new ArrayList<TuplePair>();
      }
    }
  }

  @Override
  public void shutdown() {
    closed.set(true);
  }

  @Override
  public Object getResult() {
    return closed.get() ? -1 : 0;
  }
}
