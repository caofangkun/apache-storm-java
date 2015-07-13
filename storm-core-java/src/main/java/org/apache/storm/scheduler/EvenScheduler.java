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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

@ClojureClass(className = "backtype.storm.scheduler.EvenScheduler")
public class EvenScheduler implements IScheduler {

  private static final Logger LOG = LoggerFactory
      .getLogger(EvenScheduler.class);

  @ClojureClass(className = "backtype.storm.scheduler.EvenScheduler#get-alive-assigned-node+port->executors")
  public static Map<WorkerSlot, List<ExecutorDetails>> getAliveAssignedWorkerSlotExecutors(
      Cluster cluster, String topologyId) {
    SchedulerAssignment existingAssignment =
        cluster.getAssignmentById(topologyId);
    Map<ExecutorDetails, WorkerSlot> executorToSlot = null;
    if (existingAssignment != null) {
      executorToSlot = existingAssignment.getExecutorToSlot();
    }
    Map<WorkerSlot, List<ExecutorDetails>> result =
        new HashMap<WorkerSlot, List<ExecutorDetails>>();
    if (executorToSlot != null) {
      for (Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet()) {
        List<ExecutorDetails> cache = result.get(entry.getValue());
        if (cache == null) {
          cache = new ArrayList<ExecutorDetails>();
          result.put(entry.getValue(), cache);
        }
        cache.add(entry.getKey());
      }
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.scheduler.EvenScheduler#schedule-topology")
  private static Map<ExecutorDetails, WorkerSlot> scheduleTopology(
      TopologyDetails topology, Cluster cluster) {
    // available-slots
    List<WorkerSlot> availableSlots = cluster.getAvailableSlots();
    // all-executors
    Set<ExecutorDetails> allExecutors =
        (Set<ExecutorDetails>) topology.getExecutors();
    // alive-assigned
    Map<WorkerSlot, List<ExecutorDetails>> aliveAssigned =
        getAliveAssignedWorkerSlotExecutors(cluster, topology.getId());
    int totalSlotsToUse =
        Math.min(topology.getNumWorkers(), availableSlots.size()
            + aliveAssigned.size());

    List<WorkerSlot> sortedList =
        SchedulerUtils.sortSlots(availableSlots, cluster);
    if (null == sortedList) {
      LOG.error("Available slots isn't enough for this topology:{}",
          topology.getName());
      return new HashMap<ExecutorDetails, WorkerSlot>();
    }
    // reassign-slots
    List<WorkerSlot> reassignSlots =
        sortedList.subList(0, totalSlotsToUse - aliveAssigned.size());

    // reassign-executors
    Set<ExecutorDetails> aliveExecutors = new HashSet<ExecutorDetails>();
    for (List<ExecutorDetails> cache : aliveAssigned.values()) {
      aliveExecutors.addAll(cache);
    }
    Set<ExecutorDetails> reassignExecutors =
        CoreUtil.set_difference(allExecutors, aliveExecutors);

    // user=> (map vector (list 1 2 3 4 5 6 ) (apply concat (repeat 5 (list 6701
    // 6702 6703 6704))))
    // ([1 6701] [2 6702] [3 6703] [4 6704] [5 6701] [6 6702])
    Map<ExecutorDetails, WorkerSlot> reassignment =
        new HashMap<ExecutorDetails, WorkerSlot>();
    int length = reassignSlots.size();
    if (length == 0) {
      return reassignment;
    }
    reassignment =
        SchedulerUtils.assignExecutor2Worker(reassignExecutors, reassignSlots);

    if (reassignment.size() != 0) {
      LOG.info("Available slot: {}", availableSlots.toString());
    }
    return reassignment;
  }

  @ClojureClass(className = "backtype.storm.scheduler.EvenScheduler#schedule-topologies-evenly")
  public static void scheduleTopologiesEvenly(Topologies topologies,
      Cluster cluster) {
    List<TopologyDetails> needsSchedulingTopologies =
        cluster.needsSchedulingTopologies(topologies);
    for (TopologyDetails topology : needsSchedulingTopologies) {
      String topologyId = topology.getId();

      // new-assignment
      Map<ExecutorDetails, WorkerSlot> newAssignment =
          scheduleTopology(topology, cluster);

      // node+port->executors
      Map<WorkerSlot, List<ExecutorDetails>> nodePortToExecutors =
          CoreUtil.reverse_map(newAssignment);

      for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : nodePortToExecutors
          .entrySet()) {
        WorkerSlot nodePort = entry.getKey();
        List<ExecutorDetails> executors = entry.getValue();
        cluster.assign(nodePort, topologyId, executors);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf) {
  }

  @Override
  public void schedule(Topologies topologies, Cluster cluster) {
    scheduleTopologiesEvenly(topologies, cluster);
  }
}
