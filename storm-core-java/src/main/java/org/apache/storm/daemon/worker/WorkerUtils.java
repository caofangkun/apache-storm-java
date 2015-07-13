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
package org.apache.storm.daemon.worker;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.daemon.common.Assignment;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.timer.Timer;
import org.apache.storm.util.CoreConfig;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.messaging.ConnectionWithStatus;
import backtype.storm.messaging.ConnectionWithStatus.Status;
import backtype.storm.messaging.IConnection;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.ThriftTopologyUtils;
import backtype.storm.utils.Utils;

import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.WaitStrategy;

public class WorkerUtils {
  private static Logger LOG = LoggerFactory.getLogger(WorkerUtils.class);

  @ClojureClass(className = "backtype.storm.daemon.worker#component->stream->fields")
  public static Map<String, Map<String, Fields>> componentToStreamToFields(
      StormTopology topology) {
    HashMap<String, Map<String, Fields>> componentToStreamToFields =
        new HashMap<String, Map<String, Fields>>();
    Set<String> components = ThriftTopologyUtils.getComponentIds(topology);
    for (String component : components) {
      componentToStreamToFields.put(component,
          streamToFields(topology, component));
    }
    return componentToStreamToFields;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#stream->fields")
  public static Map<String, Fields> streamToFields(StormTopology topology,
      String component) {

    Map<String, Fields> streamToFieldsMap = new HashMap<String, Fields>();

    Map<String, StreamInfo> streamInfoMap =
        ThriftTopologyUtils.getComponentCommon(topology, component)
            .get_streams();
    for (Entry<String, StreamInfo> entry : streamInfoMap.entrySet()) {
      String s = entry.getKey();
      StreamInfo info = entry.getValue();
      streamToFieldsMap.put(s, new Fields(info.get_output_fields()));
    }

    return streamToFieldsMap;

  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-default-resources")
  public static Map<String, Object> mkDefaultResources(Map conf) {
    Map<String, Object> result = new HashMap<String, Object>();
    int threadPoolSize =
        CoreUtil.parseInt(
            conf.get(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE), 4);
    result.put(WorkerTopologyContext.SHARED_EXECUTOR,
        Executors.newFixedThreadPool(threadPoolSize));
    return result;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-user-resources")
  public static Map<String, Object> mkUserResources(Map conf) {
    // TODO need to invoke a hook provided by the topology,
    // giving it a chance to create user resources.
    // this would be part of the initialization hook need to separate
    // workertopologycontext into WorkerContext and WorkerUserContext.
    // actually just do it via interfaces. just need to make sure to hide
    // setResource from tasks
    return new HashMap<String, Object>();
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-halting-timer")
  public static Timer mkHaltingTimer(String timerName) {
    return new Timer(new DefaultKillFn(20, "Error when processing an event"),
        timerName);

  }

  /**
   * Check whether this messaging connection is ready to send data
   * 
   * @param connection
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#is-connection-ready")
  public static boolean isConnectionReady(IConnection connection) {
    if (connection instanceof ConnectionWithStatus) {
      Status status = ((ConnectionWithStatus) connection).status();
      return status.equals(ConnectionWithStatus.Status.Ready);
    }
    return true;
  }

  /**
   * all connections are ready
   * 
   * @param worker WorkerData
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#all-connections-ready")
  public static boolean allConnectionsReady(WorkerData worker) {
    Collection<IConnection> connections =
        worker.getCachedNodeportToSocket().values();
    for (IConnection connection : connections) {
      if (!isConnectionReady(connection)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.worker#read-worker-executors")
  public static Set<ExecutorInfo> readWorkerExecutors(Map stormConf,
      StormClusterState stormClusterState, String stormId, String assignmentId,
      int port) throws Exception {
    Map<Integer, Integer> virtualToRealPort =
        (Map<Integer, Integer>) stormConf.get(CoreConfig.STORM_VIRTUAL_REAL_PORTS);
    if (virtualToRealPort.containsKey(port)) {
      port = virtualToRealPort.get(port);
    }
    LOG.info("Reading Assignments.");
    Assignment assignment = stormClusterState.assignmentInfo(stormId, null);
    if (assignment == null) {
      String errMsg = "Failed to get Assignment of " + stormId;
      LOG.error(errMsg);
      throw new RuntimeException(errMsg);
    }
    Map<ExecutorInfo, WorkerSlot> executorToNodePort =
        assignment.getExecutorToNodeport();
    Set<ExecutorInfo> executors = new HashSet<ExecutorInfo>();

    // executors.add(new ExecutorInfo(-1, -1));

    for (Map.Entry<ExecutorInfo, WorkerSlot> enp : executorToNodePort
        .entrySet()) {
      ExecutorInfo executor = enp.getKey();
      WorkerSlot nodeport = enp.getValue();
      if (nodeport.getNodeId().equals(assignmentId)
          && nodeport.getPort() == port) {
        executors.add(executor);
      }
    }
    if (executors.size() == 0) {
      LOG.warn("No Executors running current on port:{} and assignment:{}",
          port, assignment);
    }
    return executors;

  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-receive-queue-map")
  public static Map<ExecutorInfo, DisruptorQueue> mkReceiveQueueMap(
      Map stormConf, Set<ExecutorInfo> executors) {
    Map<ExecutorInfo, DisruptorQueue> result =
        new HashMap<ExecutorInfo, DisruptorQueue>();
    int bufferSize =
        CoreUtil.parseInt(
            stormConf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 1024);
    WaitStrategy waitStrategy =
        (WaitStrategy) Utils.newInstance((String) stormConf
            .get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
    // TODO: this depends on the type of executor
    for (ExecutorInfo e : executors) {
      DisruptorQueue queue =
          new DisruptorQueue("receive-queue-" + e.get_task_start(),
              new MultiThreadedClaimStrategy(bufferSize), waitStrategy);
      result.put(e, queue);
    }
    return result;
  }

  public static Map<Integer, DisruptorQueue> mkShortExecutorReceiveQueueMap(
      Map<ExecutorInfo, DisruptorQueue> executorReceiveQueueMap) {
    Map<Integer, DisruptorQueue> result =
        new HashMap<Integer, DisruptorQueue>();
    for (Map.Entry<ExecutorInfo, DisruptorQueue> ed : executorReceiveQueueMap
        .entrySet()) {
      ExecutorInfo e = ed.getKey();
      DisruptorQueue dq = ed.getValue();
      result.put(e.get_task_start(), dq);
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-outbound-tasks")
  public static Set<Integer> workerOutboundTasks(WorkerData workerData)
      throws Exception {
    // Returns seq of task-ids that receive messages from this worker

    WorkerTopologyContext context = Common.workerContext(workerData);
    Set<Integer> rtn = new HashSet<Integer>();

    Set<Integer> taskIds = workerData.getTaskids();
    Set<String> components = new HashSet<String>();
    for (Integer taskId : taskIds) {
      String componentId = context.getComponentId(taskId);
      // streamId to componentId to the Grouping used
      Map<String, Map<String, Grouping>> targets =
          context.getTargets(componentId);
      // componentId to the Grouping used
      for (Map<String, Grouping> e : targets.values()) {
        components.addAll(e.keySet());
      }
    }

    Map<String, List<Integer>> reverseMap =
        CoreUtil.reverse_map(workerData.getTasksToComponent());
    if (reverseMap != null) {
      for (String k : components) {
        if (reverseMap.containsKey(k)) {
          rtn.addAll(reverseMap.get(k));
        }
      }
    }
    return rtn;
  }
}
