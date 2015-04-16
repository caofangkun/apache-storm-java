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
package org.apache.storm.daemon.worker.executor;

import java.util.ArrayList;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.util.thread.RunnableCallback;

import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.MutableObject;

import com.lmax.disruptor.EventHandler;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#start-batch-transfer->worker-handler!")
public class BatchTransferToWorkerHandler extends RunnableCallback implements
    EventHandler<Object> {
  private static final long serialVersionUID = 1L;

  private TransferFn workerTransferFn;
  private KryoTupleSerializer serializer;
  private DisruptorQueue batchTransferQueue;
  private MutableObject cachedEmit = new MutableObject(
      new ArrayList<TuplePair>());
  private ExecutorData executorData;

  public BatchTransferToWorkerHandler(WorkerData worker,
      ExecutorData executorData) {
    this.executorData = executorData;
    this.batchTransferQueue = executorData.getBatchTransferQueue();
    this.workerTransferFn = new TransferFn(worker);
    this.serializer =
        new KryoTupleSerializer(executorData.getStormConf(),
            executorData.getWorkerContext());
    this.batchTransferQueue.consumerStarted();
  }

  @Override
  public void run() {
    while (!executorData.getTaskStatus().isShutdown()) {
      batchTransferQueue.consumeBatchWhenAvailable(this);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onEvent(Object o, long seqId, boolean isBatchEnd)
      throws Exception {
    if (o == null) {
      return;
    }
    ArrayList<TuplePair> aList = (ArrayList<TuplePair>) cachedEmit.getObject();
    aList.add((TuplePair) o);
    if (isBatchEnd) {
      workerTransferFn.transfer(serializer, aList);
      cachedEmit.setObject(new ArrayList<TuplePair>());
    }
  }

  @Override
  public Object getResult() {
    return executorData.getTaskStatus().isShutdown() ? -1 : 0;
  }
}
