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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.ClojureClass;
import backtype.storm.Config;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.spout.ISpout;

import com.tencent.jstorm.daemon.builtinMetrics.BuiltinMetrics;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.stats.CommonStats;
import com.tencent.jstorm.stats.SpoutExecutorStats;
import com.tencent.jstorm.stats.Stats;
import com.tencent.jstorm.utils.ServerUtils;

@ClojureClass(className = "backtype.storm.daemon.executor#ack-spout-msg")
public class AckSpoutMsg implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(AckSpoutMsg.class);

  private ISpout spout;
  private Object msgId;
  private Integer taskId;
  private String stream;
  private CommonStats task_stats;
  private Long timeDelta;
  private Long id;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private TaskData taskData;
  private Boolean isDebug = false;

  public AckSpoutMsg(ExecutorData executorData, TaskData taskData,
      Object msgId, TupleInfo tupleInfo, Long timeDelta, Long id) {
    this.taskData = taskData;
    this.spout = (ISpout) taskData.getObject();
    this.msgId = msgId;
    this.taskId = taskData.getTaskId();
    this.timeDelta = timeDelta;
    this.id = id;
    this.stormConf = executorData.getStormConf();
    this.stream = tupleInfo.getStream();
    this.task_stats = executorData.getStats();
    this.isDebug =
        ServerUtils.parseBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
  }

  public void run() {
    if (isDebug) {
      LOG.info("SPOUT Acking message id:{} msgId:{}", id, msgId);
    }

    spout.ack(msgId);

    // apply-hooks
    TaskUtils.applyHooks(taskData.getUserContext(), new SpoutAckInfo(msgId,
        taskId, timeDelta));

    if (timeDelta != null) {
      // BuiltinMetrics.spoutAckedTuple(m, stats, stream, latencyMs);
      Stats.spoutAckedTuple((SpoutExecutorStats) task_stats, stream, timeDelta);
    }
  }
}
