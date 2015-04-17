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
package org.apache.storm.daemon.worker.executor.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.worker.executor.ExecutorData;

import backtype.storm.Config;
import backtype.storm.hooks.ITaskHook;


@ClojureClass(className = "backtype.storm.daemon.task#mk-task")
public class Task {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static TaskData mkTask(ExecutorData executorData, Integer taskId)
      throws Exception {

    TaskData taskData = new TaskData(executorData, taskId);
    Map stormConf = executorData.getStormConf();

    List<String> kclasses =
        (List<String>) stormConf.get(Config.TOPOLOGY_AUTO_TASK_HOOKS);
    if (kclasses != null) {
      for (String kclass : kclasses) {
        taskData.getUserContext().addTaskHook(
            (ITaskHook) Class.forName(kclass).newInstance());
      }
    }

    // when this is called, the threads for the executor haven't been
    // started
    // yet, so we won't be risking trampling on the single-threaded claim
    // strategy disruptor queue
    List<Object> values = new ArrayList<Object>();
    values.add("startup");
    TaskUtils.sendUnanchored(taskData, Common.SYSTEM_STREAM_ID, values);
    return taskData;
  }
}
