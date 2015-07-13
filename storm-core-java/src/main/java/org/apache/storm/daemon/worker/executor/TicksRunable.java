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
import java.util.Arrays;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.tuple.TuplePair;
import org.apache.storm.util.thread.RunnableCallback;

import backtype.storm.Constants;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.DisruptorQueue;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#setup-ticks!#fn")
public class TicksRunable extends RunnableCallback {

  private static final long serialVersionUID = 1L;
  private DisruptorQueue receiveQueue;
  private WorkerTopologyContext context;
  private Integer tickTimeSecs;

  public TicksRunable(ExecutorData executorData, Integer tickTimeSecs) {
    this.receiveQueue = executorData.getReceiveQueue();
    this.context = executorData.getWorkerContext();
    this.tickTimeSecs = tickTimeSecs;
  }

  @Override
  public void run() {
    //Constants.SYSTEM_TASK_ID
    TupleImpl tupleImpl =
        new TupleImpl(context, Arrays.asList((Object) tickTimeSecs),
            -1, Constants.SYSTEM_TICK_STREAM_ID);
    TuplePair tm = new TuplePair(null, tupleImpl);
    ArrayList<TuplePair> pairs = new ArrayList<TuplePair>();
    pairs.add(tm);
    receiveQueue.publish(pairs);
  }
}
