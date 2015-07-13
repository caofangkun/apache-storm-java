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
package org.apache.storm.daemon.worker.executor.grouping;

import java.util.List;
import java.util.Set;

import org.apache.storm.guava.collect.Lists;
import org.apache.storm.guava.collect.Sets;
import org.apache.storm.util.CoreUtil;

import backtype.storm.task.WorkerTopologyContext;

public class LocalOrShuffleGrouper implements IGrouper {

  private List<Integer> targetTasks;
  private WorkerTopologyContext context;

  public LocalOrShuffleGrouper(WorkerTopologyContext context,
      List<Integer> targetTasks) {
    this.context = context;
    this.targetTasks = targetTasks;
  }

  @Override
  public List<Integer> fn(Integer taskId, List<Object> values) {
    Set<Integer> sameTasks =
        CoreUtil.SetIntersection(Sets.newHashSet(targetTasks),
            Sets.newHashSet(context.getThisWorkerTasks()));
    if (!sameTasks.isEmpty()) {
      return new ShuffleGrouper(Lists.newArrayList(sameTasks)).fn(taskId,
          values);
    } else {
      return new ShuffleGrouper(targetTasks).fn(taskId, values);
    }
  }

}
