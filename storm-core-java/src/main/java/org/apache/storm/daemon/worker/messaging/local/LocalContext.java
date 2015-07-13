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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.ClojureClass;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.IContext;
import backtype.storm.messaging.TaskMessage;

@ClojureClass(className = "backtype.storm.messaging.local#LocalContext")
public class LocalContext implements IContext {

  private ConcurrentHashMap<String, LinkedBlockingQueue<TaskMessage>> queuesMap;
  private Object lock;

  public LocalContext(
      ConcurrentHashMap<String, LinkedBlockingQueue<TaskMessage>> queuesMap,
      Object lock) {
    this.queuesMap = queuesMap;
    this.lock = lock;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf) {
    queuesMap =
        new ConcurrentHashMap<String, LinkedBlockingQueue<TaskMessage>>();
    lock = new Object();
  }

  @Override
  public IConnection bind(String stormId, int port) {
    return new LocalConnection(stormId, port, queuesMap, lock,
        LocalUtils.addQueue(queuesMap, lock, stormId, port));
  }

  @Override
  public IConnection connect(String stormId, String host, int port) {
    return new LocalConnection(stormId, port, queuesMap, lock, null);
  }

  @Override
  public void term() {
  }

}
