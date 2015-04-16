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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.storm.ClojureClass;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.ExecutorData;
import org.apache.storm.guava.collect.Lists;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.JavaObject;
import backtype.storm.generated.ShellComponent;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.BoltAckInfo;
import backtype.storm.hooks.info.BoltExecuteInfo;
import backtype.storm.hooks.info.BoltFailInfo;
import backtype.storm.hooks.info.SpoutAckInfo;
import backtype.storm.hooks.info.SpoutFailInfo;
import backtype.storm.spout.ShellSpout;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.utils.Utils;

import com.lmax.disruptor.InsufficientCapacityException;

public class TaskUtils {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.task#mk-topology-context-builder")
  private static TopologyContext mkTopologyContextBuilder(WorkerData worker,
      ExecutorData executorData, StormTopology topology, Integer taskId)
      throws Exception {
    Map conf = worker.getConf();
    String stormId = worker.getTopologyId();
    String stormroot = ConfigUtil.supervisorStormdistRoot(conf, stormId);
    String resourcePath = ConfigUtil.supervisorStormResourcesPath(stormroot);
    String workerPidsRoot =
        ConfigUtil.workerPidsRoot(conf, worker.getWorkerId());
    TopologyContext context =
        new TopologyContext(topology, worker.getStormConf(),
            worker.getTasksToComponent(), worker.getComponentToSortedTasks(),
            worker.getComponentToStreamToFields(), stormId, resourcePath,
            workerPidsRoot, taskId, worker.getPort(), Lists.newArrayList(worker
                .getTaskids()), worker.getDefaultSharedResources(),
            worker.getUserSharedResources(),
            executorData.getSharedExecutorData(),
            executorData.getIntervalToTaskToMetricRegistry(),
            executorData.getOpenOrPrepareWasCalled());

    return context;
  }

  @ClojureClass(className = "backtype.storm.daemon.task#system-topology-context")
  public static TopologyContext systemTopologyContext(WorkerData worker,
      ExecutorData executorData, Integer taskId) throws Exception {
    return mkTopologyContextBuilder(worker, executorData,
        worker.getSystemTopology(), taskId);
  }

  @ClojureClass(className = "backtype.storm.daemon.task#user-topology-context")
  public static TopologyContext userTopologyContext(WorkerData worker,
      ExecutorData executorData, Integer taskId) throws Exception {
    return mkTopologyContextBuilder(worker, executorData, worker.getTopology(),
        taskId);
  }

  @ClojureClass(className = "backtype.storm.daemon.task#get-task-object")
  public static Object getTaskObject(StormTopology topology, String component_id) {
    Map<String, SpoutSpec> spouts = topology.get_spouts();
    Map<String, Bolt> bolts = topology.get_bolts();
    Map<String, StateSpoutSpec> state_spouts = topology.get_state_spouts();
    ComponentObject obj = null;
    if (spouts.containsKey(component_id)) {
      obj = spouts.get(component_id).get_spout_object();
    } else if (bolts.containsKey(component_id)) {
      obj = bolts.get(component_id).get_bolt_object();
    } else if (state_spouts.containsKey(component_id)) {
      obj = state_spouts.get(component_id).get_state_spout_object();
    }
    if (obj == null) {
      throw new RuntimeException("Could not find " + component_id + " in "
          + topology.toString());
    }
    Object componentObject = Utils.getSetComponentObject(obj);
    Object rtn = null;
    if (componentObject instanceof JavaObject) {
      rtn = Thrift.instantiateJavaObject((JavaObject) componentObject);
    } else if (componentObject instanceof ShellComponent) {
      if (spouts.containsKey(component_id)) {
        rtn = new ShellSpout((ShellComponent) componentObject);
      } else {
        rtn = new ShellBolt((ShellComponent) componentObject);
      }
    } else {
      rtn = componentObject;
    }
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.daemon.task#apply-hooks")
  public static void applyHooks(TopologyContext topologyContext,
      SpoutAckInfo spoutAckInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.spoutAck(spoutAckInfo);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.task#apply-hooks#spoutFail")
  public static void applyHooks(TopologyContext topologyContext,
      SpoutFailInfo spoutFailInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.spoutFail(spoutFailInfo);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.task#apply-hooks#boltExecute")
  public static void applyHooks(TopologyContext topologyContext,
      BoltExecuteInfo boltExecuteInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.boltExecute(boltExecuteInfo);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.task#apply-hooks#boltAck")
  public static void applyHooks(TopologyContext topologyContext,
      BoltAckInfo boltAckInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.boltAck(boltAckInfo);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.task#apply-hooks#boltFail")
  public static void applyHooks(TopologyContext topologyContext,
      BoltFailInfo boltFailInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.boltFail(boltFailInfo);
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.task#send-unanchored")
  public static void sendUnanchored(TaskData taskData, String stream,
      List<Object> values, ConcurrentLinkedQueue<TuplePair> overflowBuffer)
      throws InsufficientCapacityException {
    // TODO: this is all expensive... should be precomputed
    TasksFn tasksFn = taskData.getTasksFn();
    List<Integer> targetTaskIds = tasksFn.fn(stream, values);
    if (targetTaskIds.size() == 0) {
      return;
    }

    TopologyContext topologyContext = taskData.getSystemContext();
    ExecutorTransferFn transferFn = taskData.getExecutorData().getTransferFn();
    TupleImpl outTuple =
        new TupleImpl(topologyContext, values, topologyContext.getThisTaskId(),
            stream);

    for (Integer t : targetTaskIds) {
      transferFn.transfer(t, outTuple, overflowBuffer);
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.task#send-unanchored")
  public static void sendUnanchored(TaskData taskData, String stream,
      List<Object> values) throws InsufficientCapacityException {
    sendUnanchored(taskData, stream, values, null);
  }
}
