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
package org.apache.storm.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

@ClojureClass(className = "backtype.storm.scheduler.DefaultScheduler")
public class DefaultScheduler implements IScheduler {
  @ClojureClass(className = "backtype.storm.scheduler.DefaultScheduler#slots-can-reassign")
  private Set<WorkerSlot> slotsCanReassign(Cluster cluster,
      Set<WorkerSlot> slots) {
    Set<WorkerSlot> result = new HashSet<WorkerSlot>();
    for (WorkerSlot slot : slots) {
      if (!cluster.isBlackListed(slot.getNodeId())) {
        SupervisorDetails sd = cluster.getSupervisorById(slot.getNodeId());
        if (null != sd) {
          Set<Integer> ap = sd.getAllPorts();
          if (null != ap && ap.contains(slot.getPort())) {
            result.add(slot);
          }
        }
      }
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.scheduler.DefaultScheduler#bad-slots")
  private Set<WorkerSlot> badSlots(
      Map<WorkerSlot, List<ExecutorDetails>> existingSlots, int numExecutors,
      int numWorkers) {
    if (numWorkers != 0) {
      Map<Integer, Integer> distribution =
          CoreUtil.integerDivided(numExecutors, numWorkers);
      Map<WorkerSlot, List<ExecutorDetails>> result =
          new HashMap<WorkerSlot, List<ExecutorDetails>>();
      for (Entry<WorkerSlot, List<ExecutorDetails>> entry : existingSlots
          .entrySet()) {
        Integer executorCount = distribution.get(entry.getValue().size());
        if (executorCount != null && executorCount > 0) {
          result.put(entry.getKey(), entry.getValue());
          executorCount--;
          distribution.put(entry.getValue().size(), executorCount);
        }
      }
      for (WorkerSlot slot : result.keySet())
        existingSlots.remove(slot);
      return existingSlots.keySet();
    }
    return null;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void prepare(Map conf) {
  }

  @ClojureClass(className = "backtype.storm.scheduler.DefaultScheduler#-schedule")
  @Override
  public void schedule(Topologies topologies, Cluster cluster) {
    List<TopologyDetails> needsSchedulingTopologies =
        cluster.needsSchedulingTopologies(topologies);
    for (TopologyDetails details : needsSchedulingTopologies) {
      List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
      Set<ExecutorDetails> allExecutors =
          (Set<ExecutorDetails>) details.getExecutors();
      Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned =
          EvenScheduler.getAliveAssignedWorkerSlotExecutors(cluster,
              details.getId());
      Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
      for (List<ExecutorDetails> cache : aliveAssigned.values()) {
        aliveExecutors.addAll(cache);
      }
      Set<WorkerSlot> canReassignSlots =
          slotsCanReassign(cluster, aliveAssigned.keySet());
      int totalSlotsToUse =
          Math.min(details.getNumWorkers(), canReassignSlots.size()
              + availableSlots.size());
      Set<WorkerSlot> badSlot = null;
      if (aliveAssigned.size() < totalSlotsToUse
          || !allExecutors.equals(aliveExecutors))
        badSlot = badSlots(aliveAssigned, allExecutors.size(), totalSlotsToUse);
      if (badSlot != null)
        cluster.freeSlots(badSlot);
      Map<String, TopologyDetails> topologiesCache =
          new HashMap<String, TopologyDetails>();
      topologiesCache.put(details.getId(), details);
      EvenScheduler.scheduleTopologiesEvenly(new Topologies(topologiesCache),
          cluster);
    }
  }

}
