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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

@ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler")
public class IsolationScheduler implements IScheduler {

  private final static Logger LOG = LoggerFactory
      .getLogger(IsolationScheduler.class);

  @SuppressWarnings("rawtypes")
  private Map conf;

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf) {
    this.conf = conf;
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#-schedule")
  @Override
  public void schedule(Topologies topologies, Cluster cluster) {

    SchedulerUtils.filterDeadAssignments(cluster);
    // step 1
    Set<String> origBlackList = cluster.getBlacklistedHosts();
    // get isolation topologies
    List<TopologyDetails> isolationTopos =
        isolationTopologies(topologies.getTopologies());
    // isolation topology ids
    Set<String> isolationTopoIds = getTopoplogyIds(isolationTopos);
    // assign executor to worker for every isolation topology
    Map<String, Set<Set<ExecutorDetails>>> isoTopoWorkerSpecs =
        topologyWorkerSpecs(isolationTopos);
    // according the configuration, compute the number that worker in a
    // supervisor
    Map<String, Map<Integer, Integer>> machineDistributions =
        topologyMachineDistribution(isolationTopos);

    Map<String, List<AssignmentInfo>> hostAssignments =
        hostAssignments(cluster);
    //
    Set<Map.Entry<String, List<AssignmentInfo>>> hostAssignEntries =
        hostAssignments.entrySet();
    // step 2
    for (Map.Entry<String, List<AssignmentInfo>> entry : hostAssignEntries) {
      List<AssignmentInfo> assInfo = entry.getValue();
      String topologyId = assInfo.get(0).getTopologyId();
      Map<Integer, Integer> distribution = machineDistributions.get(topologyId);
      Set<Set<ExecutorDetails>> isolExecutors =
          isoTopoWorkerSpecs.get(topologyId);
      int workerNum = assInfo.size();

      if (isolationTopoIds.contains(topologyId)
          && checkAssignmentTopology(assInfo, topologyId)
          && (null != distribution && distribution.containsKey(workerNum))
          && checkAssignmentWorkerSpecs(assInfo, isolExecutors)) {

        decrementDistribution(distribution, workerNum);
        for (AssignmentInfo ass : assInfo) {
          isolExecutors.remove(ass.getExecutors());
        }
        cluster.blacklistHost(entry.getKey());
      } else {
        for (AssignmentInfo ass : assInfo) {
          if (isolationTopoIds.contains(ass.getTopologyId())) {
            cluster.freeSlot(ass.getWorkerSlot());
          }
        }
      }
    }

    // step 3
    Map<String, Set<WorkerSlot>> hostUsedSlots = getHostUsedSlots(cluster);
    LinkedList<HostAssignableSlots> hss = hostAssignableSlots(cluster);
    for (Map.Entry<String, Set<Set<ExecutorDetails>>> entry : isoTopoWorkerSpecs
        .entrySet()) {
      String topologyId = entry.getKey();
      Set<Set<ExecutorDetails>> executorSet = entry.getValue();
      List<Integer> workerNum =
          distributionSortedAmts(machineDistributions.get(topologyId));
      for (Integer w : workerNum) {
        HostAssignableSlots hostSlots = hss.peek();
        List<WorkerSlot> ws =
            null != hostSlots ? hostSlots.getWorkerSlots() : null;

        if (null != ws && ws.size() >= w.intValue()) {
          hss.poll();
          cluster.freeSlots(hostUsedSlots.get(hostSlots.getHostName()));
          for (WorkerSlot tmpSlot : ws.subList(0, w)) {
            Set<ExecutorDetails> es = removeSetElement(executorSet);
            cluster.assign(tmpSlot, topologyId, es);
          }
          cluster.blacklistHost(hostSlots.getHostName());
        }
      }
    }
    // step 4
    List<String> failedTopologyIds =
        getFailedIsoTopologyIds(isoTopoWorkerSpecs);
    if (failedTopologyIds.size() > 0) {
      LOG.warn("Unable to isolate topologies "
          + failedTopologyIds
          + ". No machine had enough worker slots to run the remaining workers for these topologies. "
          + "Clearing all other resources and will wait for enough resources for "
          + "isolated topologies before allocating any other resources.");
      Map<String, Set<WorkerSlot>> usedSlots = getHostUsedSlots(cluster);
      Set<Map.Entry<String, Set<WorkerSlot>>> entries = usedSlots.entrySet();
      for (Map.Entry<String, Set<WorkerSlot>> entry : entries) {
        if (!cluster.isBlacklistedHost(entry.getKey())) {
          cluster.freeSlots(entry.getValue());
        }
      }
    } else {
      //
      Set<String> allocatedTopologies = allocatedTopologies(isoTopoWorkerSpecs);
      Topologies leftOverTopologies =
          leftoverTopologies(topologies, allocatedTopologies);
      (new DefaultScheduler()).schedule(leftOverTopologies, cluster);
    }
    cluster.setBlacklistedHosts(origBlackList);
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#isolated-topologies")
  @SuppressWarnings("unchecked")
  private List<TopologyDetails> isolationTopologies(
      Collection<TopologyDetails> topologyDetails) {
    Object isolatedMachines = conf.get(Config.ISOLATION_SCHEDULER_MACHINES);

    if (null != isolatedMachines) {
      Map<Object, Object> im = (Map<Object, Object>) isolatedMachines;
      Set<Object> keys = im.keySet();

      Set<String> topoNames = new HashSet<String>();
      for (Object key : keys) {
        topoNames.add(String.valueOf(key));
      }
      // filter the isolation topologies
      List<TopologyDetails> isolationTopos = new ArrayList<TopologyDetails>();
      for (TopologyDetails td : topologyDetails) {
        if (topoNames.contains(td.getName())) {
          isolationTopos.add(td);
        }
      }
      return isolationTopos;
    }
    return null;
  }

  private Set<String> getTopoplogyIds(List<TopologyDetails> topologies) {
    Set<String> ids = new HashSet<String>();

    if (null != topologies && topologies.size() > 0) {
      for (TopologyDetails topology : topologies) {
        ids.add(topology.getId());
      }
    }
    return ids;
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#topology-worker-specs")
  private Map<String, Set<Set<ExecutorDetails>>> topologyWorkerSpecs(
      List<TopologyDetails> topologies) {
    Map<String, Set<Set<ExecutorDetails>>> workerSpecs =
        new HashMap<String, Set<Set<ExecutorDetails>>>();

    for (TopologyDetails topology : topologies) {
      workerSpecs.put(topology.getId(), computeWorkerSpecs(topology));
    }
    return workerSpecs;
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#host-assignments")
  private Map<String, List<AssignmentInfo>> hostAssignments(Cluster cluster) {
    Map<String, SchedulerAssignment> assignments = cluster.getAssignments();
    Collection<SchedulerAssignment> assignmentValues = assignments.values();
    Map<String, List<AssignmentInfo>> hostAssignments =
        new HashMap<String, List<AssignmentInfo>>();

    for (SchedulerAssignment sa : assignmentValues) {
      Map<ExecutorDetails, WorkerSlot> executorSlots = sa.getExecutorToSlot();
      Map<WorkerSlot, List<ExecutorDetails>> slotExecutors =
          CoreUtil.reverse_map(executorSlots);
      Set<Map.Entry<WorkerSlot, List<ExecutorDetails>>> entries =
          slotExecutors.entrySet();
      for (Map.Entry<WorkerSlot, List<ExecutorDetails>> entry : entries) {
        WorkerSlot ws = entry.getKey();
        List<ExecutorDetails> executors = entry.getValue();
        String host = cluster.getHost(ws.getNodeId());
        //
        AssignmentInfo ass =
            new AssignmentInfo(ws, sa.getTopologyId(),
                new HashSet<ExecutorDetails>(executors));
        List<AssignmentInfo> executorList = hostAssignments.get(host);
        if (null == executorList) {
          executorList = new ArrayList<AssignmentInfo>();
          hostAssignments.put(host, executorList);
        }
        executorList.add(ass);
      }
    }
    return hostAssignments;
  }

  /**
   * Take a toplogy, assign executors to workers
   * 
   * @param details topology details
   * @return the set of executor for every worker
   */
  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#compute-worker-specs")
  private Set<Set<ExecutorDetails>> computeWorkerSpecs(TopologyDetails details) {

    Map<ExecutorDetails, String> executorComp =
        details.getExecutorToComponent();
    Map<String, List<ExecutorDetails>> compExecutors =
        CoreUtil.reverse_map(executorComp);

    List<ExecutorDetails> allExecutors = new ArrayList<ExecutorDetails>();
    Collection<List<ExecutorDetails>> values = compExecutors.values();
    for (List<ExecutorDetails> eList : values) {
      allExecutors.addAll(eList);
    }

    int numWorkers = details.getNumWorkers();
    int bucketIndex = 0;
    Map<Integer, Set<ExecutorDetails>> bucketExecutors =
        new HashMap<Integer, Set<ExecutorDetails>>(numWorkers);
    for (ExecutorDetails executor : allExecutors) {
      Set<ExecutorDetails> executorSets = bucketExecutors.get(bucketIndex);
      if (null == executorSets) {
        executorSets = new HashSet<ExecutorDetails>();
        bucketExecutors.put(Integer.valueOf(bucketIndex), executorSets);
      }
      executorSets.add(executor);
      // update bucketIndex;
      bucketIndex = (++bucketIndex) % numWorkers;
    }

    Collection<Set<ExecutorDetails>> bucketExecutorValues =
        bucketExecutors.values();

    return new HashSet<Set<ExecutorDetails>>(bucketExecutorValues);
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#topology-machine-distribution")
  private Map<String, Map<Integer, Integer>> topologyMachineDistribution(
      List<TopologyDetails> isolationTopos) {

    Map<String, Map<Integer, Integer>> machineDistributions =
        new HashMap<String, Map<Integer, Integer>>();
    for (TopologyDetails topology : isolationTopos) {
      machineDistributions.put(topology.getId(), machineDistribution(topology));
    }
    return machineDistributions;
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#machine-distribution")
  private Map<Integer, Integer> machineDistribution(TopologyDetails topology) {
    Object machineConf = conf.get(Config.ISOLATION_SCHEDULER_MACHINES);
    if (null != machineConf) {
      @SuppressWarnings("rawtypes")
      int machineNum =
          CoreUtil.parseInt(((Map) machineConf).get(topology.getName()), 0);

      if (machineNum > 0) {
        int workerNum = topology.getNumWorkers();
        TreeMap<Integer, Integer> distribution =
            CoreUtil.integerDivided(workerNum, machineNum);

        if (distribution.containsKey(0)) {
          distribution.remove(0);
        }
        return distribution;
      }

    }
    return null;
  }

  private boolean checkAssignmentTopology(List<AssignmentInfo> assignments,
      String topologyId) {
    for (AssignmentInfo assignment : assignments) {
      if (!topologyId.equals(assignment.getTopologyId())) {
        return false;
      }
    }
    return true;
  }

  private boolean checkAssignmentWorkerSpecs(List<AssignmentInfo> assigments,
      Set<Set<ExecutorDetails>> workerSpecs) {
    for (AssignmentInfo ass : assigments) {
      if (!workerSpecs.contains(ass.getExecutors())) {
        return false;
      }
    }
    return true;
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#decrement-distribution!")
  private void decrementDistribution(Map<Integer, Integer> distribution,
      int value) {
    Integer distValue = distribution.get(value);
    if (null != distValue) {
      int newValue = distValue - 1;
      if (0 == newValue) {
        distribution.remove(value);
      } else {
        distribution.put(value, newValue);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#host->used-slots")
  private Map<String, Set<WorkerSlot>> getHostUsedSlots(Cluster cluster) {
    Collection<WorkerSlot> usedSlots = cluster.getUsedSlots();
    Map<String, Set<WorkerSlot>> hostUsedSlots =
        new HashMap<String, Set<WorkerSlot>>();
    for (WorkerSlot ws : usedSlots) {
      String host = cluster.getHost(ws.getNodeId());
      Set<WorkerSlot> slots = hostUsedSlots.get(host);
      if (null == slots) {
        slots = new HashSet<WorkerSlot>();
        hostUsedSlots.put(host, slots);
      }
      slots.add(ws);
    }
    return hostUsedSlots;
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#host->host-assignable-slots")
  private LinkedList<HostAssignableSlots> hostAssignableSlots(Cluster cluster) {
    List<WorkerSlot> assignableSlots = cluster.getAssignableSlots();
    Map<String, List<WorkerSlot>> hostAssignableSlots =
        new HashMap<String, List<WorkerSlot>>();
    for (WorkerSlot as : assignableSlots) {
      String host = cluster.getHost(as.getNodeId());

      List<WorkerSlot> workerSlots = hostAssignableSlots.get(host);
      if (null == workerSlots) {
        workerSlots = new ArrayList<WorkerSlot>();
        hostAssignableSlots.put(host, workerSlots);
      }
      workerSlots.add(as);
    }
    List<HostAssignableSlots> hostAssignSlotList =
        new ArrayList<HostAssignableSlots>();
    for (Map.Entry<String, List<WorkerSlot>> entry : hostAssignableSlots
        .entrySet()) {
      hostAssignSlotList.add(new HostAssignableSlots(entry.getKey(), entry
          .getValue()));
    }
    // sort
    Collections.sort(hostAssignSlotList, new Comparator<HostAssignableSlots>() {

      @Override
      public int compare(HostAssignableSlots o1, HostAssignableSlots o2) {
        return o2.getWorkerSlots().size() - o1.getWorkerSlots().size();
      }
    });
    // shuffle
    Collections.shuffle(hostAssignSlotList);

    return new LinkedList<HostAssignableSlots>(hostAssignSlotList);
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#distribution->sorted-amts")
  private List<Integer> distributionSortedAmts(
      Map<Integer, Integer> distributions) {
    List<Integer> sorts = new ArrayList<Integer>();
    for (Map.Entry<Integer, Integer> entry : distributions.entrySet()) {
      int workers = entry.getKey();
      int machines = entry.getValue();
      for (int i = 0; i < machines; i++) {
        sorts.add(workers);
      }
    }
    Collections.sort(sorts, new Comparator<Integer>() {

      @Override
      public int compare(Integer o1, Integer o2) {
        return o1.intValue() - o2.intValue();
      }
    });

    return sorts;
  }

  // for each isolated topology:
  // compute even distribution of executors -> workers on the number of workers
  // specified for the topology
  // compute distribution of workers to machines
  // determine host -> list of [slot, topology id, executors]
  // iterate through hosts and: a machine is good if:
  // 1. only running workers from one isolated topology
  // 2. all workers running on it match one of the distributions of executors
  // for that topology
  // 3. matches one of the # of workers
  // blacklist the good hosts and remove those workers from the list of need to
  // be assigned workers
  // otherwise unassign all other workers for isolated topologies if assigned
  //
  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#remove-elem-from-set!")
  public Set<ExecutorDetails> removeSetElement(
      Set<Set<ExecutorDetails>> workerSpec) {
    if (workerSpec.iterator().hasNext()) {
      Set<ExecutorDetails> elem = workerSpec.iterator().next();
      workerSpec.remove(elem);
      return elem;
    } else {
      return null;
    }

  }

  private List<String> getFailedIsoTopologyIds(
      Map<String, Set<Set<ExecutorDetails>>> isoTopologyWorkerSpecs) {
    Set<Map.Entry<String, Set<Set<ExecutorDetails>>>> entries =
        isoTopologyWorkerSpecs.entrySet();
    List<String> failedIds = new ArrayList<String>();
    for (Map.Entry<String, Set<Set<ExecutorDetails>>> entry : entries) {
      Set<Set<ExecutorDetails>> executors = entry.getValue();
      if (null != executors && executors.size() > 0) {
        // if()
        failedIds.add(entry.getKey());
      }
    }
    return failedIds;
  }

  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#allocated-topologies")
  private Set<String> allocatedTopologies(
      Map<String, Set<Set<ExecutorDetails>>> workerSpecs) {
    Set<String> alloTopos = new HashSet<String>();
    Set<Map.Entry<String, Set<Set<ExecutorDetails>>>> entries =
        workerSpecs.entrySet();
    for (Map.Entry<String, Set<Set<ExecutorDetails>>> entry : entries) {
      if (entry.getValue().isEmpty()) {
        alloTopos.add(entry.getKey());
      }
    }
    return alloTopos;
  }

  //
  @ClojureClass(className = "backtype.storm.scheduler.IsolationScheduler#leftover-topologies")
  private Topologies leftoverTopologies(Topologies topologies,
      Set<String> filterIdsSet) {
    Collection<TopologyDetails> details = topologies.getTopologies();
    Map<String, TopologyDetails> leftTopologies =
        new HashMap<String, TopologyDetails>();
    for (TopologyDetails detail : details) {
      String tId = detail.getId();
      if (!filterIdsSet.contains(tId)) {
        leftTopologies.put(tId, detail);
      }
    }
    return new Topologies(leftTopologies);
  }

  class AssignmentInfo {

    private WorkerSlot workerSlot;
    private String topologyId;
    private Set<ExecutorDetails> executors;

    public AssignmentInfo(WorkerSlot workerSlot, String topologyId,
        Set<ExecutorDetails> executors) {
      super();
      this.workerSlot = workerSlot;
      this.topologyId = topologyId;
      this.executors = executors;
    }

    public WorkerSlot getWorkerSlot() {
      return workerSlot;
    }

    public void setWorkerSlot(WorkerSlot workerSlot) {
      this.workerSlot = workerSlot;
    }

    public String getTopologyId() {
      return topologyId;
    }

    public void setTopologyId(String topologyId) {
      this.topologyId = topologyId;
    }

    public Set<ExecutorDetails> getExecutors() {
      return executors;
    }

    public void setExecutors(Set<ExecutorDetails> executors) {
      this.executors = executors;
    }

  }

  class HostAssignableSlots {
    private String hostName;
    private List<WorkerSlot> workerSlots;

    public HostAssignableSlots(String hostName, List<WorkerSlot> workerSlots) {
      super();
      this.hostName = hostName;
      this.workerSlots = workerSlots;
    }

    public String getHostName() {
      return hostName;
    }

    public void setHostName(String hostName) {
      this.hostName = hostName;
    }

    public List<WorkerSlot> getWorkerSlots() {
      return workerSlots;
    }

    public void setWorkerSlots(List<WorkerSlot> workerSlots) {
      this.workerSlots = workerSlots;
    }

  }
}
