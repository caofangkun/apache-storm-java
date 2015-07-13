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
package org.apache.storm.daemon.worker.transfer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.TransferDrainer;

import com.lmax.disruptor.EventHandler;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-tuples-handler")
public class TransferTuplesHandler extends RunnableCallback implements
    EventHandler<Object> {
  private static final long serialVersionUID = 1L;
  private final static Logger LOG = LoggerFactory
      .getLogger(TransferTuplesHandler.class);
  private DisruptorQueue transferQueue;
  private HashMap<Integer, String> taskToNodePort;
  private HashMap<String, IConnection> nodeportToSocket;
  private AtomicBoolean workerActive;
  private ReentrantReadWriteLock.ReadLock endpointSocketReadLock;
  private TransferDrainer drainer;
  private Exception exception = null;

  // TODO: consider having a max batch size besides what disruptor does
  // automagically to prevent latency issues
  public TransferTuplesHandler(WorkerData workerData) {
    this.transferQueue = workerData.getTransferQueue();
    this.taskToNodePort = workerData.getCachedTaskToNodeport();
    this.nodeportToSocket = workerData.getCachedNodeportToSocket();
    this.workerActive = workerData.getStormActiveAtom();
    this.endpointSocketReadLock = workerData.getEndpointSocketLock().readLock();
    this.drainer = new TransferDrainer();
    this.transferQueue.consumerStarted();
  }

  @Override
  public void run() {
    try {
      transferQueue.consumeBatchWhenAvailable(this);
    } catch (Throwable e) {
      exception = new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onEvent(Object packets, long sequence, boolean endOfBatch)
      throws Exception {
    if (packets == null) {
      return;
    }
    drainer.add((HashMap<Integer, ArrayList<TaskMessage>>) packets);
    if (endOfBatch) {
      endpointSocketReadLock.lock();
      try {
        drainer.send(taskToNodePort, nodeportToSocket);
      } finally {
        endpointSocketReadLock.unlock();
      }
      drainer.clear();
    }
  }

  @Override
  public Exception error() {
    if (null != exception) {
      LOG.error("TransferTuples Exception {}",
          CoreUtil.stringifyError(exception));
    }
    return exception;
  }

  @Override
  public void shutdown() {
    workerActive.set(false);
  }

  @Override
  public Object getResult() {
    return workerActive.get() ? 0 : -1;
  }
}
