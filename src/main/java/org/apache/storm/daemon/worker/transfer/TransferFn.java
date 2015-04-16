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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.tuple.TuplePair;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.DisruptorQueue;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn")
public class TransferFn {
  private static Logger LOG = LoggerFactory.getLogger(TransferFn.class);
  private Set<Integer> localTasks;
  private TransferLocalFn localTransfer;
  private DisruptorQueue transferQueue;
  private boolean trySerializeLocal;
  private WorkerData workerData;

  public TransferFn(WorkerData workerData) {
    this.localTasks = workerData.getTaskids();
    this.localTransfer = new TransferLocalFn(workerData);
    this.transferQueue = workerData.getTransferQueue();
    this.trySerializeLocal =
        CoreUtil.parseBoolean(
            workerData.getStormConf().get(
                Config.TOPOLOGY_TESTING_ALWAYS_TRY_SERIALIZE), false);
    this.workerData = workerData;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn")
  public void transfer(KryoTupleSerializer serializer,
      ArrayList<TuplePair> tupleBatch) {
    if (trySerializeLocal) {
      LOG.warn("WILL TRY TO SERIALIZE ALL TUPLES (Turn off TOPOLOGY-TESTING-ALWAYS-TRY-SERIALIZE for production)");
      trySerializeLocalTransferFn(serializer, tupleBatch);
    } else {
      transferFn(serializer, tupleBatch);
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn#fn")
  public void transferFn(KryoTupleSerializer serializer,
      ArrayList<TuplePair> tupleBatch) {
    if (tupleBatch == null) {
      return;
    }
    List<TuplePair> local = new ArrayList<TuplePair>();
    HashMap<WorkerSlot, ArrayList<TaskMessage>> remoteMap =
        new HashMap<WorkerSlot, ArrayList<TaskMessage>>();
    for (TuplePair pair : tupleBatch) {
      Integer task = pair.getOutTask();
      if (localTasks.contains(task)) {
        local.add(pair);
      } else {
        // Using java objects directly to avoid performance issues in java code
        ConcurrentHashMap<Integer, WorkerSlot> taskToNodePort =
            workerData.getCachedTaskToNodeport();
        WorkerSlot nodePort = taskToNodePort.get(task);
        // if nodePort is null, dropped messages
        if (null != nodePort) {
          if (!remoteMap.containsKey(nodePort)) {
            remoteMap.put(nodePort, new ArrayList<TaskMessage>());
          }
          ArrayList<TaskMessage> remote = remoteMap.get(nodePort);
          try {
            TaskMessage message =
                new TaskMessage(task, serializer.serialize((TupleImpl) pair
                    .getOutTuple()));
            remote.add(message);
          } catch (Throwable t) {
            LOG.error("Serialize tuple failed.", t);
          }
        }
      }
    }

    if (local.size() > 0) {
      localTransfer.transfer(local);
    }
    if (remoteMap.size() > 0) {
      transferQueue.publish(remoteMap);
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-fn#try-serialize-local#fn")
  public void trySerializeLocalTransferFn(KryoTupleSerializer serializer,
      ArrayList<TuplePair> tupleBatch) {
    assertCanSerialize(serializer, tupleBatch);
    transfer(serializer, tupleBatch);
  }

  /**
   * Check that all of the tuples can be serialized by serializing them.
   * 
   * @param serializer
   * @param tupleBatch
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#assert-can-serialize")
  private void assertCanSerialize(KryoTupleSerializer serializer,
      ArrayList<TuplePair> tupleBatch) {
    for (TuplePair pair : tupleBatch) {
      // TODO
      Tuple tuple = (Tuple) pair.getOutTuple();
      serializer.serialize(tuple);
    }
  }
}