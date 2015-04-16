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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.ClojureClass;

import backtype.storm.messaging.TaskMessage;

public class LocalUtils {

  @ClojureClass(className = "backtype.storm.messaging.local#add-queue!")
  public static LinkedBlockingQueue<TaskMessage> addQueue(
      ConcurrentHashMap<String, LinkedBlockingQueue<TaskMessage>> queueMap,
      Object lock, String stormId, int port) {
    String id = stormId + "-" + port;
    synchronized (lock) {
      if (!queueMap.containsKey(id)) {
        queueMap.put(id, new LinkedBlockingQueue<TaskMessage>());
      }
    }
    return queueMap.get(id);
  }

  @ClojureClass(className = "backtype.storm.messaging.local#mk-context")
  public static LocalContext mkContext() {
    LocalContext context = new LocalContext(null, null);
    context.prepare(null);
    return context;
  }
}
