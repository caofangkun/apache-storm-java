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
package org.apache.storm.daemon.worker.messaging.local;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;

@ClojureClass(className = "backtype.storm.messaging.local#LocalConnection")
public class LocalConnection implements IConnection {
  private static Logger LOG = LoggerFactory.getLogger(LocalConnection.class);

  private String stormId;
  private int port;
  private ConcurrentHashMap<String, LinkedBlockingQueue<TaskMessage>> queuesMap;
  private Object lock;
  private LinkedBlockingQueue<TaskMessage> queue;

  public LocalConnection(String stormId, int port,
      ConcurrentHashMap<String, LinkedBlockingQueue<TaskMessage>> queuesMap,
      Object lock, LinkedBlockingQueue<TaskMessage> queue) {
    this.stormId = stormId;
    this.port = port;
    this.queuesMap = queuesMap;
    this.lock = lock;
    this.queue = queue;
  }

  @Override
  public Iterator<TaskMessage> recv(int flags, int clientId) {
    if (queue == null) {
      throw new IllegalArgumentException("Cannot receive on this socket");
    }
    List<TaskMessage> ret = new ArrayList<TaskMessage>();
    TaskMessage msg = null;
    try {
      if (flags == 1) {
        msg = queue.poll();
      } else {
        msg = queue.take();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    if (msg != null) {
      ret.add(msg);
      return ret.iterator();
    }
    return null;
  }

  @Override
  public void send(int taskId, byte[] payload) {
    LinkedBlockingQueue<TaskMessage> sendQueue =
        LocalUtils.addQueue(queuesMap, lock, stormId, port);
    TaskMessage taskMessage = new TaskMessage(taskId, payload);
    try {
      sendQueue.put(taskMessage);
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
  }

  @Override
  public void send(Iterator<TaskMessage> iter) {
    LinkedBlockingQueue<TaskMessage> sendQueue =
        LocalUtils.addQueue(queuesMap, lock, stormId, port);
    while (iter.hasNext()) {
      try {
        sendQueue.put(iter.next());
      } catch (InterruptedException e) {
        LOG.error("LocalConnection send error {}",
            CoreUtil.stringifyError(e));
      }
    }
  }

  @Override
  public void close() {
  }

}
