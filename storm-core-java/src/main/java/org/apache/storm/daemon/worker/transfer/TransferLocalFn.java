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
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.tuple.TuplePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.DisruptorQueue;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-transfer-local-fn")
public class TransferLocalFn {
  private static Logger LOG = LoggerFactory.getLogger(TransferLocalFn.class);

  private WorkerData workerData;
  private Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap;
  private Map<Integer, Integer> taskToShortExecutor;
  private KryoTupleDeserializer deserializer;

  public TransferLocalFn(WorkerData workerData) {
    this.workerData = workerData;
    this.shortExecutorReceiveQueueMap =
        workerData.getShortExecutorReceiveQueueMap();
    this.taskToShortExecutor = workerData.getTaskToShortExecutor();
    try {
      WorkerTopologyContext workerContext = Common.workerContext(workerData);
      this.deserializer =
          new KryoTupleDeserializer(workerData.getStormConf(), workerContext);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
  }

  public void transfer(List<TuplePair> tupleBatch) {
    if (tupleBatch == null) {
      return;
    }
    Map<Integer, List<TuplePair>> ret = new HashMap<Integer, List<TuplePair>>();
    for (TuplePair e : tupleBatch) {

      Integer key = taskToShortExecutor.get(e.getOutTask());
      if (key != null) {
        List<TuplePair> curr = ret.get(key);
        if (curr == null) {
          curr = new ArrayList<TuplePair>();
        }
        curr.add(e);

        ret.put(key, curr);
      } else {
        TupleImpl tupleImpl =
            (TupleImpl) deserializer.deserialize((byte[]) e.getOutTuple());
        LOG.warn("Received invalid messages from task {} ,valid tasks are {}",
            e.getOutTask() + "-" + tupleImpl.getSourceTask(),
            taskToShortExecutor.keySet());
      }
    }
    for (Map.Entry<Integer, List<TuplePair>> r : ret.entrySet()) {
      DisruptorQueue q = shortExecutorReceiveQueueMap.get(r.getKey());
      if (q != null) {
        q.publish(r.getValue());
      } else {
        LOG.warn(
            "Received invalid messages for unknown tasks {}. Dropping... ",
            r.getKey());
      }
    }
  }

  public WorkerData getWorkerData() {
    return workerData;
  }

  public void setWorkerData(WorkerData workerData) {
    this.workerData = workerData;
  }
}