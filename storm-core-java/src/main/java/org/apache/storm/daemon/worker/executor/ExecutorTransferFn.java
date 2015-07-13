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

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.tuple.TuplePair;

import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.DisruptorQueue;

import com.lmax.disruptor.InsufficientCapacityException;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-executor-transfer-fn")
public class ExecutorTransferFn {
  private DisruptorQueue batchtransferToWorker;

  public ExecutorTransferFn(DisruptorQueue batchtransferToWorker) {
    this.batchtransferToWorker = batchtransferToWorker;
  }

  public void transfer(Integer outTask, TupleImpl tuple, boolean isBlock,
      ConcurrentLinkedQueue<TuplePair> overflowBuffer)
      throws InsufficientCapacityException {
    TuplePair tuplePair = new TuplePair(outTask, tuple);
    if (overflowBuffer != null && !overflowBuffer.isEmpty()) {
      overflowBuffer.add(tuplePair);
    } else {
      try {
        batchtransferToWorker.publish(tuplePair, isBlock);
      } catch (InsufficientCapacityException e) {
        if (overflowBuffer != null) {
          overflowBuffer.add(tuplePair);
        } else {
          throw e;
        }
      }
    }
  }

  public void transfer(Integer task, TupleImpl tuple,
      ConcurrentLinkedQueue<TuplePair> overflowBuffer)
      throws InsufficientCapacityException {
    transfer(task, tuple, overflowBuffer == null, overflowBuffer);
  }

  public void transfer(Integer task, TupleImpl tuple)
      throws InsufficientCapacityException {
    transfer(task, tuple, null);
  }

}
