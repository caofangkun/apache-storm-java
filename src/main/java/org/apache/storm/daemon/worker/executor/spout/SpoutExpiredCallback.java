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
package org.apache.storm.daemon.worker.executor.spout;

import java.util.Map;

import backtype.storm.ClojureClass;
import backtype.storm.utils.RotatingMap;

import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.utils.ServerUtils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-threads:spout#pending#reify RotatingMap$ExpiredCallback")
public class SpoutExpiredCallback<K, V> implements
    RotatingMap.ExpiredCallback<K, V> {
  private ExecutorData executorData;
  private Map<Integer, TaskData> taskDatas;

  public SpoutExpiredCallback(ExecutorData executorData,
      Map<Integer, TaskData> taskDatas) {
    this.executorData = executorData;
    this.taskDatas = taskDatas;
  }

  @Override
  public void expire(K key, V val) {
    // key:msgid val:[task-id spout-id tuple-info start-time-ms]
    if (key == null || val == null) {
      return;
    }
    Long id = (Long) key;
    TupleInfo tupleInfo = (TupleInfo) val;
    Integer taskId = tupleInfo.getTaskId();
    Object spoutId = tupleInfo.getMessageId();
    Long startTimeMs = tupleInfo.getTimestamp();
    Long timeDelta = null;
    if (startTimeMs != null) {
      timeDelta = ServerUtils.time_delta_ms(startTimeMs);
    }
    FailSpoutMsg fsm =
        new FailSpoutMsg(executorData, taskDatas.get(taskId), spoutId,
            tupleInfo, timeDelta, "TIMEOUT", id);
    fsm.run();
  }
}
