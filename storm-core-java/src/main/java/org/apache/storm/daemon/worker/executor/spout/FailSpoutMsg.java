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

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.ExecutorData;
import org.apache.storm.daemon.worker.executor.task.TaskData;
import org.apache.storm.daemon.worker.executor.task.TaskUtils;
import org.apache.storm.daemon.worker.stats.CommonStats;
import org.apache.storm.daemon.worker.stats.SpoutExecutorStats;
import org.apache.storm.daemon.worker.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.spout.ISpout;

@ClojureClass(className = "backtype.storm.daemon.executor#fail-spout-msg")
public class FailSpoutMsg implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(FailSpoutMsg.class);
  private ISpout spout;
  private TupleInfo tupleInfo;
  private CommonStats task_stats;
  private Integer taskId;
  private Object msgId;
  private Long timeDelta;
  private String reason;
  private Long id;
  private TaskData taskData;

  public FailSpoutMsg(ExecutorData executorData, TaskData taskData,
      Object msgId, TupleInfo tupleInfo, Long timeDelta, String reason, Long id) {
    this.taskData = taskData;
    this.spout = (ISpout) taskData.getObject();
    this.taskId = taskData.getTaskId();
    this.tupleInfo = tupleInfo;
    this.msgId = tupleInfo.getMessageId();
    this.timeDelta = timeDelta;
    this.reason = reason;
    this.id = id;
    this.task_stats = executorData.getStats();
  }

  public void run() {
    // TODO: need to throttle these when there's lots of failures
    LOG.debug("SPOUT Failing " + id + ": " + tupleInfo.getValues().toString()
        + " REASON: " + reason + " MSG-ID: " + msgId);

    spout.fail(msgId);

    // apply-hooks
    TaskUtils.applyHooks(taskData.getUserContext(), new SpoutFailInfo(msgId,
        taskId, timeDelta));

    if (timeDelta != null) {
      // BuiltinMetrics.spoutFailedTuple(m, stats, stream, latencyMs);
      Stats.spoutFailedTuple((SpoutExecutorStats) task_stats,
          tupleInfo.getStream(), timeDelta);
    }
  }
}
