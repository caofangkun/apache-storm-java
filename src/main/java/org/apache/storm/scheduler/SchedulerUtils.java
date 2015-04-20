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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

public class SchedulerUtils {
  public static Map<ExecutorDetails, WorkerSlot> assignExecutor2Worker(
      Set<ExecutorDetails> topologyExecutors, List<WorkerSlot> workers) {

    List<ExecutorDetails> executorList =
        new ArrayList<ExecutorDetails>(topologyExecutors);
    //
    Collections.sort(executorList, new Comparator<ExecutorDetails>() {
      @Override
      public int compare(ExecutorDetails o1, ExecutorDetails o2) {
        return o1.getStartTask() - o2.getStartTask();
      }
    });
    int executorSize = executorList.size();
    List<WorkerSlot> tmpSlots = new ArrayList<WorkerSlot>(executorSize);
    int numWorkers = workers.size();
    for (int i = 0; i < executorSize; i++) {
      tmpSlots.add(workers.get(i % numWorkers));
    }

    // List
    Iterator<WorkerSlot> workerIter = tmpSlots.iterator();
    Iterator<ExecutorDetails> executorIter = executorList.iterator();

    Map<ExecutorDetails, WorkerSlot> executor2Worker =
        new HashMap<ExecutorDetails, WorkerSlot>();
    while (workerIter.hasNext() && executorIter.hasNext()) {
      executor2Worker.put(executorIter.next(), workerIter.next());
    }
    return executor2Worker;
  }

  public static void divideDeadAssigments(int unusedSlots,
      Map<ExecutorDetails, WorkerSlot> deadAssignments,
      Map<ExecutorDetails, WorkerSlot> remains,
      Map<ExecutorDetails, WorkerSlot> reassigns) {

    int index = 1;
    for (Map.Entry<ExecutorDetails, WorkerSlot> entry : deadAssignments
        .entrySet()) {
      if (index < unusedSlots) {
        reassigns.put(entry.getKey(), entry.getValue());
        index++;
      } else {
        remains.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static Set<WorkerSlot> getDeadSlots(Cluster cluster) {
    Set<WorkerSlot> deadSlots = new HashSet<WorkerSlot>();
    Map<String, SupervisorDetails> supervisors = cluster.getSupervisors();
    if (null != supervisors && !supervisors.isEmpty()) {
      Set<Map.Entry<String, SupervisorDetails>> entries =
          supervisors.entrySet();
      for (Map.Entry<String, SupervisorDetails> entry : entries) {
        SupervisorDetails sd = entry.getValue();
        Set<Integer> alivePorts = sd.getAllPorts();
        Set<Integer> allPorts = (Set<Integer>) sd.getMeta();

        Set<Integer> deadPorts = CoreUtil.set_difference(allPorts, alivePorts);
        if (null != deadPorts && deadPorts.size() > 0) {
          for (Integer dp : deadPorts) {
            deadSlots.add(new WorkerSlot(entry.getKey(), dp));
          }
        }
      }
    }
    return deadSlots.size() > 0 ? deadSlots : null;
  }

  public static void filterDeadAssignments(Cluster cluster) {
    Set<WorkerSlot> deadSlots = getDeadSlots(cluster);
    if (null != deadSlots && deadSlots.size() > 0) {
      // filter dead assignments
      Map<String, SchedulerAssignment> curAssignments =
          cluster.getAssignments();
      Set<Map.Entry<String, SchedulerAssignment>> entries =
          curAssignments.entrySet();
      for (Map.Entry<String, SchedulerAssignment> entry : entries) {
        String topologyId = entry.getKey();
        SchedulerAssignment sa = entry.getValue();
        Map<ExecutorDetails, WorkerSlot> executor2Slots =
            sa.getExecutorToSlot();
        Map<ExecutorDetails, WorkerSlot> newES =
            new HashMap<ExecutorDetails, WorkerSlot>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> es : executor2Slots
            .entrySet()) {
          WorkerSlot tmp = es.getValue();
          if (!deadSlots.contains(tmp)) {
            newES.put(es.getKey(), tmp);
          }
        }
        if (newES.size() != executor2Slots.size()) {
          // update assigments
          curAssignments.put(topologyId, new SchedulerAssignmentImpl(
              topologyId, newES));
        }
      }
    }
  }

  @ClojureClass(className = "backtype.storm.scheduler.EvenScheduler#sort-slots")
  public static List<WorkerSlot> sortSlots(List<WorkerSlot> availableSlots,
      Cluster cluster) {
    if (null != availableSlots && availableSlots.size() > 0) {
      // group by node id
      Map<String, List<WorkerSlot>> slotGroups =
          new TreeMap<String, List<WorkerSlot>>();
      for (WorkerSlot slot : availableSlots) {
        String sHost = cluster.getHost(slot.getNodeId());
        List<WorkerSlot> slotList = slotGroups.get(sHost);
        if (null == slotList) {
          slotList = new ArrayList<WorkerSlot>();
          slotGroups.put(sHost, slotList);
        }
        slotList.add(slot);
      }
      // sort list by port
      for (List<WorkerSlot> slotList : slotGroups.values()) {
        Collections.sort(slotList, new Comparator<WorkerSlot>() {
          @Override
          public int compare(WorkerSlot o1, WorkerSlot o2) {
            return o1.getPort() - o2.getPort();
          }
        });
      }
      // sort by count
      List<List<WorkerSlot>> tmpList =
          new ArrayList<List<WorkerSlot>>(slotGroups.values());
      Collections.sort(tmpList, new Comparator<List<WorkerSlot>>() {
        @Override
        public int compare(List<WorkerSlot> o1, List<WorkerSlot> o2) {
          return o2.size() - o1.size();
        }
      });

      return CoreUtil.interleaveAll(tmpList);
    }
    return null;
  }
}
