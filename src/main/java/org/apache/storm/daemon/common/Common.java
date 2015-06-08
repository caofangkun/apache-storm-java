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
package org.apache.storm.daemon.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.storm.ClojureClass;
import org.apache.storm.cluster.StormClusterState;
import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.acker.AckerBolt;
import org.apache.storm.daemon.worker.WorkerData;
import org.apache.storm.daemon.worker.executor.grouping.GroupingType;
import org.apache.storm.guava.collect.Lists;
import org.apache.storm.guava.collect.Sets;
import org.apache.storm.thrift.Thrift;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ExecutorInfo;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormBase;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.metric.MetricsConsumerBolt;
import backtype.storm.metric.SystemBolt;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.security.auth.IAuthorizer;
import backtype.storm.task.IBolt;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.common")
public class Common {
  private final static Logger LOG = LoggerFactory.getLogger(Common.class);

  public static final String ACKER_COMPONENT_ID = AckerBolt.ACKER_COMPONENT_ID;
  public static final String ACKER_INIT_STREAM_ID =
      AckerBolt.ACKER_INIT_STREAM_ID;
  public static final String ACKER_ACK_STREAM_ID =
      AckerBolt.ACKER_ACK_STREAM_ID;
  public static final String ACKER_FAIL_STREAM_ID =
      AckerBolt.ACKER_FAIL_STREAM_ID;

  public static final String SYSTEM_STREAM_ID = "__system";

  public static final String SYSTEM_COMPONENT_ID =
      Constants.SYSTEM_COMPONENT_ID;
  public static final String SYSTEM_TICK_STREAM_ID =
      Constants.SYSTEM_TICK_STREAM_ID;
  public static final String METRICS_STREAM_ID = Constants.METRICS_STREAM_ID;
  public static final String METRICS_TICK_STREAM_ID =
      Constants.METRICS_TICK_STREAM_ID;

  public static final String LS_WORKER_HEARTBEAT = "worker-heartbeat";
  public static final String LS_ID = "supervisor-id";
  public static final String LS_SLOTS = "supervsior-ports";
  public static final String LS_LOCAL_ASSIGNMENTS = "local-assignments";
  public static final String LS_APPROVED_WORKERS = "approved-workers";

  @ClojureClass(className = "backtype.storm.daemon.common#system-id?")
  public static boolean systemId(String id) {
    return Utils.isSystemId(id);
  }

  @ClojureClass(className = "backtype.storm.daemon.common#get-storm-id")
  public static String getStormId(StormClusterState stormClusterState,
      String stormName) throws Exception {
    Set<String> activeStorms = stormClusterState.activeStorms();
    if (activeStorms == null) {
      return null;
    }
    for (String stormId : activeStorms) {
      StormBase stormBase = stormClusterState.stormBase(stormId, null);
      if (stormBase.get_name().equals(stormName)) {
        return stormId;
      }
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.daemon.common#topology-bases")
  public static HashMap<String, StormBase> topologyBases(
      StormClusterState stormClusterState) throws Exception {
    HashMap<String, StormBase> rtn = new HashMap<String, StormBase>();
    Set<String> activeTopologies = stormClusterState.activeStorms();
    if (activeTopologies != null) {
      for (String id : activeTopologies) {
        StormBase base = stormClusterState.stormBase(id, null);
        if (base != null) {
          rtn.put(id, base);
        }
      }
    }
    return rtn;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.common#validate-distributed-mode!")
  public static void validateDistributedMode(Map conf) {
    if (ConfigUtil.isLocalMode(conf)) {
      throw new IllegalArgumentException("Cannot start server in local mode!");
    }
  }

  // Check Whether ID of Bolt or spout is system_id
  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.daemon.common#validate-ids!")
  private static void validateIds(StormTopology topology)
      throws InvalidTopologyException {
    List<String> list = new ArrayList<String>();
    for (StormTopology._Fields field : Thrift.STORM_TOPOLOGY_FIELDS) {
      Object value = topology.getFieldValue(field);
      if (value != null) {
        Map<String, Object> obj_map = (Map<String, Object>) value;
        Set<String> commponentIds = obj_map.keySet();
        for (String id : commponentIds) {
          if (systemId(id)) {
            throw new InvalidTopologyException(id
                + " is not a valid component id");
          }
        }
        for (Object obj : obj_map.values()) {
          validateComponent(obj);
        }
        list.addAll(commponentIds);
      }
    }

    List<String> offending = CoreUtil.anyIntersection(list);
    if (offending.isEmpty() == false) {
      throw new InvalidTopologyException("Duplicate component ids: "
          + offending);
    }
  }

  /**
   * 
   * @param topology
   * @return componentId --> component
   */
  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.daemon.common#all-components")
  public static Map<String, Object> allComponents(StormTopology topology) {
    Map<String, Object> result = new HashMap<String, Object>();
    for (StormTopology._Fields field : Thrift.STORM_TOPOLOGY_FIELDS) {
      Object fields = topology.getFieldValue(field);
      if (fields != null) {
        result.putAll((Map<? extends String, ? extends Object>) fields);
      }
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.common#component-conf")
  public static Map componentConf(ComponentCommon common) {
    return (Map) CoreUtil.from_json(common.get_json_conf());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.common#validate-basic!")
  public static void validateBasic(StormTopology topology)
      throws InvalidTopologyException {

    validateIds(topology);

    for (StormTopology._Fields field : Thrift.SPOUT_FIELDS) {
      Object value = topology.getFieldValue(field);
      if (value != null) {
        Map<String, Object> obj_map = (Map<String, Object>) value;
        for (Object obj : obj_map.values()) {
          validateComponentInputs(obj);
        }
      }
    }

    Collection<Object> comps = allComponents(topology).values();
    for (Object obj : comps) {
      ComponentCommon common = null;
      if (obj instanceof StateSpoutSpec) {
        StateSpoutSpec spec = (StateSpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof SpoutSpec) {
        SpoutSpec spec = (SpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof Bolt) {
        Bolt spec = (Bolt) obj;
        common = spec.get_common();
      }

      Map conf = componentConf(common);
      int parallelismHint = Thrift.parallelismHint(common);
      Integer topologyTasks =
          CoreUtil.parseInt(conf.get(Config.TOPOLOGY_TASKS));
      if (topologyTasks != null && topologyTasks > 0 && parallelismHint <= 0) {
        throw new InvalidTopologyException(
            "Number of executors must be greater than 0 when number of tasks is greater than 0");
      }
    }
  }

  /**
   * validate all the component subscribe from component+stream which actually
   * exists in the topology and if it is a fields grouping, validate the
   * corresponding field exists
   * 
   * @param topology
   * @throws InvalidTopologyException
   */
  @ClojureClass(className = "backtype.storm.daemon.common#validate-structure!")
  private static void validateStructure(StormTopology topology)
      throws InvalidTopologyException {
    Map<String, Object> allComponents = allComponents(topology);
    for (Map.Entry<String, Object> entry : allComponents.entrySet()) {
      String id = entry.getKey();
      Object obj = entry.getValue();
      ComponentCommon common = null;
      if (obj instanceof StateSpoutSpec) {
        StateSpoutSpec spec = (StateSpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof SpoutSpec) {
        SpoutSpec spec = (SpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof Bolt) {
        Bolt spec = (Bolt) obj;
        common = spec.get_common();
      }

      Map<GlobalStreamId, Grouping> inputs =
          new HashMap<GlobalStreamId, Grouping>();
      if (common != null) {
        inputs = common.get_inputs();
      }

      for (Map.Entry<GlobalStreamId, Grouping> input : inputs.entrySet()) {
        GlobalStreamId globalStreamId = input.getKey();
        Grouping grouping = input.getValue();
        String sourceComponentId = globalStreamId.get_componentId();
        String sourceStreamId = globalStreamId.get_streamId();

        if (!allComponents.containsKey(sourceComponentId)) {
          throw new InvalidTopologyException("Component: [" + id
              + "] subscribes from non-existent component ["
              + sourceComponentId + "]");
        }

        Object currentObj = allComponents.get(sourceComponentId);
        ComponentCommon currentCommon = null;
        if (currentObj instanceof StateSpoutSpec) {
          StateSpoutSpec spec = (StateSpoutSpec) currentObj;
          currentCommon = spec.get_common();
        }
        if (currentObj instanceof SpoutSpec) {
          SpoutSpec spec = (SpoutSpec) currentObj;
          currentCommon = spec.get_common();
        }
        if (currentObj instanceof Bolt) {
          Bolt spec = (Bolt) currentObj;
          currentCommon = spec.get_common();
        }
        Map<String, StreamInfo> sourceStreams =
            new HashMap<String, StreamInfo>();
        if (currentCommon != null) {
          sourceStreams = currentCommon.get_streams();
        }
        if (!sourceStreams.containsKey(sourceStreamId)) {
          throw new InvalidTopologyException("Component: [" + id
              + "] subscribes from non-existent stream: [" + sourceStreamId
              + "] of component [" + sourceComponentId + "]");
        }

        GroupingType groupingType = Thrift.groupingType(grouping);
        if (groupingType.equals(GroupingType.fields)) {
          Set<String> groupingFields = Sets.newHashSet(grouping.get_fields());
          Set<String> sourceStreamFields =
              Sets.newHashSet(sourceStreams.get(sourceStreamId)
                  .get_output_fields());
          Set<String> diffFields =
              CoreUtil.set_difference(groupingFields, sourceStreamFields);
          if (diffFields != null && diffFields.size() != 0) {
            throw new InvalidTopologyException("Component: [" + id
                + "] subscribes from stream: [" + sourceStreamId
                + "] of component [" + sourceComponentId
                + "] with non-existent fields: " + diffFields);
          }
        }
      }
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.common#acker-inputs")
  public static Map<GlobalStreamId, Grouping> ackerInputs(StormTopology topology) {
    Map<String, Bolt> bolt_ids = topology.get_bolts();
    Map<String, SpoutSpec> spout_ids = topology.get_spouts();

    Map<GlobalStreamId, Grouping> spout_inputs =
        new HashMap<GlobalStreamId, Grouping>();
    for (Entry<String, SpoutSpec> spout : spout_ids.entrySet()) {
      String id = spout.getKey();
      GlobalStreamId stream = new GlobalStreamId(id, ACKER_INIT_STREAM_ID);
      Grouping group = Thrift.mkFieldsGrouping(Lists.newArrayList("id"));
      spout_inputs.put(stream, group);
    }

    Map<GlobalStreamId, Grouping> bolt_inputs =
        new HashMap<GlobalStreamId, Grouping>();
    for (Entry<String, Bolt> bolt : bolt_ids.entrySet()) {
      String id = bolt.getKey();
      GlobalStreamId streamAck = new GlobalStreamId(id, ACKER_ACK_STREAM_ID);
      Grouping groupAck = Thrift.mkFieldsGrouping(Lists.newArrayList("id"));
      GlobalStreamId streamFail = new GlobalStreamId(id, ACKER_FAIL_STREAM_ID);
      Grouping groupFail = Thrift.mkFieldsGrouping(Lists.newArrayList("id"));
      bolt_inputs.put(streamAck, groupAck);
      bolt_inputs.put(streamFail, groupFail);
    }

    spout_inputs.putAll(bolt_inputs);
    return spout_inputs;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.common#add-acker!")
  public static void addAcker(Map stormConf, StormTopology ret) {
    Integer numExecutors =
        CoreUtil.parseInt(stormConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
    if (numExecutors == null) {
      numExecutors = CoreUtil.parseInt(stormConf.get(Config.TOPOLOGY_WORKERS));
    }

    HashMap<String, StreamInfo> outputs = new HashMap<String, StreamInfo>();
    outputs.put(ACKER_ACK_STREAM_ID,
        Thrift.directOutputFields(Lists.newArrayList(("id"))));
    outputs.put(ACKER_FAIL_STREAM_ID,
        Thrift.directOutputFields(Lists.newArrayList(("id"))));
    Map<GlobalStreamId, Grouping> ackerInputs = ackerInputs(ret);
    Map ackerBoltConf = new HashMap<String, Object>();
    ackerBoltConf.put(Config.TOPOLOGY_TASKS, numExecutors);
    ackerBoltConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
        stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
    Bolt ackerBolt =
        Thrift.mkBoltSpec(ackerInputs, new AckerBolt(), outputs, numExecutors,
            ackerBoltConf);

    // for bolt
    Collection<Bolt> bolts = ret.get_bolts().values();
    for (Bolt bolt : bolts) {
      ComponentCommon common = bolt.get_common();
      common.put_to_streams(ACKER_ACK_STREAM_ID,
          Thrift.outputFields(Lists.newArrayList("id", "ack-val")));
      common.put_to_streams(ACKER_FAIL_STREAM_ID,
          Thrift.outputFields(Lists.newArrayList("id")));
    }

    // for spout
    Collection<SpoutSpec> spouts = ret.get_spouts().values();
    for (SpoutSpec spout : spouts) {
      ComponentCommon common = spout.get_common();
      Map spoutConf = componentConf(common);
      if (spoutConf == null) {
        spoutConf = new HashMap<String, Object>();
      }
      spoutConf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
          stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
      // this set up tick tuples to cause timeouts to be triggered
      common.set_json_conf(CoreUtil.to_json(spoutConf));
      common.put_to_streams(ACKER_INIT_STREAM_ID, Thrift.outputFields(Lists
          .newArrayList("id", "init-val", "spout-task")));
      common.put_to_inputs(new GlobalStreamId(ACKER_COMPONENT_ID,
          ACKER_ACK_STREAM_ID), Thrift.mkDirectGrouping());
      common.put_to_inputs(new GlobalStreamId(ACKER_COMPONENT_ID,
          ACKER_FAIL_STREAM_ID), Thrift.mkDirectGrouping());
      // spout.set_common(common);
    }
    ret.put_to_bolts(ACKER_COMPONENT_ID, ackerBolt);
  }

  @ClojureClass(className = "backtype.storm.daemon.common#add-metric-streams!")
  private static void addMetricStreams(StormTopology topology) {
    // Collection<Object> component = allComponents(topology).values();
    // for (Object obj : component) {
    // ComponentCommon common = null;
    // if (obj instanceof StateSpoutSpec) {
    // StateSpoutSpec spec = (StateSpoutSpec) obj;
    // common = spec.get_common();
    // }
    // if (obj instanceof SpoutSpec) {
    // SpoutSpec spec = (SpoutSpec) obj;
    // common = spec.get_common();
    // }
    // if (obj instanceof Bolt) {
    // Bolt spec = (Bolt) obj;
    // common = spec.get_common();
    // }
    // if (common != null) {
    // List<String> msis = Lists.newArrayList("task-info", "data-points");
    // StreamInfo sinfo = Thrift.outputFields(msis);
    // common.put_to_streams(METRICS_STREAM_ID, sinfo);
    // }
    // }
  }

  @ClojureClass(className = "backtype.storm.daemon.common#add-system-streams!")
  public static void addSystemStreams(StormTopology topology) {
    Collection<Object> component = allComponents(topology).values();
    for (Object obj : component) {
      ComponentCommon common = null;
      if (obj instanceof StateSpoutSpec) {
        StateSpoutSpec spec = (StateSpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof SpoutSpec) {
        SpoutSpec spec = (SpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof Bolt) {
        Bolt spec = (Bolt) obj;
        common = spec.get_common();
      }
      if (common != null) {
        common.put_to_streams(SYSTEM_STREAM_ID,
            Thrift.outputFields(Lists.newArrayList("event")));
      }
    }
  }

  /**
   *
   * @param stormConf
   * @return Generates a list of component ids for each metrics consumer e.g.
   *         [\"__metrics_org.mycompany.MyMetricsConsumer\", ..]
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.common#metrics-consumer-register-ids")
  public static List<String> metricsConsumerRegisterIds(Map stormConf) {
    Map<String, String> metricsRegisters =
        (Map<String, String>) stormConf
            .get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER);
    List<String> ids = new ArrayList<String>();
    String prefix = Constants.METRICS_COMPONENT_ID_PREFIX;
    for (String clazz : metricsRegisters.keySet()) {
      ids.add(prefix + clazz);
    }
    return ids;
  }

  @SuppressWarnings({ "unused", "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.common#metrics-consumer-bolt-specs ")
  private static Map<String, IBolt> metricsConsumerBoltSpecs(Map stormConf,
      StormTopology topology) {
    // TODO
    Map<String, String> metircsRegisters =
        (Map<String, String>) stormConf
            .get(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER);

    Map<String, Object> components = allComponents(topology);
    Object component = components.get(SYSTEM_COMPONENT_ID);
    GlobalStreamId gsi =
        new GlobalStreamId(SYSTEM_COMPONENT_ID, METRICS_STREAM_ID);
    Map<GlobalStreamId, Grouping> inputs =
        new HashMap<GlobalStreamId, Grouping>();
    inputs.put(gsi, Thrift.mkShuffleGrouping());
    // String consumerClassName,
    // Object registrationArgument
    IBolt bolt = new MetricsConsumerBolt(null, component);
    return null;
  }

  @SuppressWarnings("rawtypes")
  private static void addMetricComponents(Map storm_conf, StormTopology topology) {
    // TODO
    // Map<String, IBolt> result = metricsConsumerBoltSpecs(storm_conf,
    // topology);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.common#add-system-components!")
  private static void addSystemComponents(Map conf, StormTopology topology) {
    Map<GlobalStreamId, Grouping> inputs =
        new HashMap<GlobalStreamId, Grouping>();
    IBolt ackerbolt = new SystemBolt();
    Map<String, StreamInfo> output = new HashMap<String, StreamInfo>();
    output.put(SYSTEM_TICK_STREAM_ID,
        Thrift.outputFields(Lists.newArrayList("rate_secs")));
    output.put(METRICS_TICK_STREAM_ID,
        Thrift.outputFields(Lists.newArrayList("interval")));
    conf.put(Config.TOPOLOGY_TASKS, 0);

    Bolt systemBoltSpec = Thrift.mkBoltSpec(inputs, ackerbolt, output, 0, conf);

    topology.put_to_bolts(SYSTEM_COMPONENT_ID, systemBoltSpec);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.common#system-topology!")
  public static StormTopology systemTopology(Map storm_conf,
      StormTopology topology) throws InvalidTopologyException {
    validateBasic(topology);
    StormTopology ret = topology.deepCopy();
    addAcker(storm_conf, ret);
    addMetricComponents(storm_conf, ret);
    addSystemComponents(storm_conf, ret);
    addMetricStreams(ret);
    addSystemStreams(ret);
    validateStructure(ret);
    return ret;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.common#has-ackers?")
  public static boolean hasAcker(Map stormConf) {
    Integer topologyAckerExecutors =
        CoreUtil.parseInt(stormConf.get(Config.TOPOLOGY_ACKER_EXECUTORS));
    return (topologyAckerExecutors == null) || (topologyAckerExecutors > 0);
  }

  /**
   * 
   * @param allComponents
   * @return Returns map from componentId -> parallelismHint
   */
  @ClojureClass(className = "backtype.storm.daemon.common#num-start-executors")
  public static Map<String, Integer> numStartExecutors(
      Map<String, Object> allComponents) {
    Map<String, Integer> numExecutors = new HashMap<String, Integer>();
    for (Entry<String, Object> entry : allComponents.entrySet()) {
      String componentId = entry.getKey();
      Object obj = entry.getValue();
      ComponentCommon common = null;
      if (obj instanceof Bolt) {
        common = ((Bolt) obj).get_common();
      } else if (obj instanceof SpoutSpec) {
        common = ((SpoutSpec) obj).get_common();
      } else if (obj instanceof StateSpoutSpec) {
        common = ((StateSpoutSpec) obj).get_common();
      } else {
        LOG.warn("Component is not valid!");
        continue;
      }
      numExecutors.put(componentId, Thrift.parallelismHint(common));
    }
    return numExecutors;
  }

  /**
   * 
   * @param userTopology
   * @param stormConf
   * @return Returns map from task_id -> component_id
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.common#storm-task-info")
  public static Map<Integer, String> stormTaskInfo(StormTopology userTopology,
      Map stormConf) throws Exception {
    Map<String, Integer> sortedMap = new TreeMap<String, Integer>();
    StormTopology systemTopoloy = systemTopology(stormConf, userTopology);
    Map<String, Object> allComponents = allComponents(systemTopoloy);
    for (Map.Entry<String, Object> c : allComponents.entrySet()) {
      String componentId = c.getKey();
      Object obj = c.getValue();
      ComponentCommon common = null;
      if (obj instanceof StateSpoutSpec) {
        StateSpoutSpec spec = (StateSpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof SpoutSpec) {
        SpoutSpec spec = (SpoutSpec) obj;
        common = spec.get_common();
      }
      if (obj instanceof Bolt) {
        Bolt spec = (Bolt) obj;
        common = spec.get_common();
      }
      if (common != null) {
        common.put_to_streams(SYSTEM_STREAM_ID,
            Thrift.outputFields(Lists.newArrayList("event")));
      }
      String jsonConf = common.get_json_conf();
      Map conf = (Map) CoreUtil.from_json(jsonConf);
      Integer numTasks = CoreUtil.parseInt(conf.get(Config.TOPOLOGY_TASKS));
      sortedMap.put(componentId, numTasks);
    }
    Map<Integer, String> result = new TreeMap<Integer, String>();
    int id = 0;
    for (Map.Entry<String, Integer> tmp : sortedMap.entrySet()) {
      String component = tmp.getKey();
      Integer numTasks = tmp.getValue();
      for (int cnt = 0; cnt < numTasks; cnt++) {
        id++;
        result.put(id, component);
      }
    }
    return result;
  }

  /**
   * @param executorInfo
   * @return taskids eg: [1,3] -> [1,2,3]
   */
  @ClojureClass(className = "backtype.storm.daemon.common#executor-id->tasks")
  public static List<Integer> executorIdToTasks(ExecutorInfo executorInfo) {
    List<Integer> taskIds = new ArrayList<Integer>();
    int firstTaskId = executorInfo.get_task_start();
    int lastTaskId = executorInfo.get_task_end();
    while (firstTaskId <= lastTaskId) {
      taskIds.add(firstTaskId);
      firstTaskId++;
    }
    return taskIds;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.common#worker-context")
  public static WorkerTopologyContext workerContext(WorkerData workerData)
      throws Exception {
    Map stormConf = workerData.getStormConf();
    String topologyId = workerData.getTopologyId();
    String workerId = workerData.getWorkerId();
    StormTopology systemTopology = workerData.getSystemTopology();
    String stormroot =
        ConfigUtil.supervisorStormdistRoot(stormConf,
            workerData.getTopologyId());
    String codeDir = ConfigUtil.supervisorStormResourcesPath(stormroot);
    String pidDir = ConfigUtil.workerPidsRoot(stormConf, workerId);
    return new WorkerTopologyContext(systemTopology, stormConf,
        workerData.getTasksToComponent(),
        workerData.getComponentToSortedTasks(),
        workerData.getComponentToStreamToFields(), topologyId, codeDir, pidDir,
        workerData.getPort(), Lists.newArrayList(workerData.getTaskids()),
        workerData.getDefaultSharedResources(),
        workerData.getUserSharedResources());
  }

  @ClojureClass(className = "backtype.storm.daemon.common#to-task->node+port")
  public static Map<Integer, WorkerSlot> toTaskToNodePort(
      Map<ExecutorInfo, WorkerSlot> executorToNodePort) {
    Map<Integer, WorkerSlot> result = new HashMap<Integer, WorkerSlot>();
    for (Map.Entry<ExecutorInfo, WorkerSlot> entry : executorToNodePort
        .entrySet()) {
      ExecutorInfo e = entry.getKey();
      WorkerSlot nodePort = entry.getValue();
      for (Integer t : Common.executorIdToTasks(e)) {
        result.put(t, nodePort);
      }
    }
    return result;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.common#mk-authorization-handler")
  public static IAuthorizer mkAuthorizationHandler(String klassname, Map conf)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {
    Class aznClass = null;
    IAuthorizer aznHandler = null;
    if (klassname != null) {
      aznClass = Class.forName(klassname);
      if (aznClass != null) {
        aznHandler = (IAuthorizer) aznClass.newInstance();
        if (aznHandler != null) {
          aznHandler.prepare(conf);
          LOG.debug("authorization class name:" + klassname + " class:"
              + aznClass + " handler:" + aznHandler);
        }
      }
    }

    return aznHandler;
  }

  private static void validateComponent(Object obj)
      throws InvalidTopologyException {
    if (obj instanceof StateSpoutSpec) {
      StateSpoutSpec spec = (StateSpoutSpec) obj;
      for (String id : spec.get_common().get_streams().keySet()) {
        if (systemId(id)) {
          throw new InvalidTopologyException(id
              + " is not a valid component id");
        }
      }
    } else if (obj instanceof SpoutSpec) {
      SpoutSpec spec = (SpoutSpec) obj;
      for (String id : spec.get_common().get_streams().keySet()) {
        if (systemId(id)) {
          throw new InvalidTopologyException(id
              + " is not a valid component id");
        }
      }
    } else if (obj instanceof Bolt) {
      Bolt spec = (Bolt) obj;
      for (String id : spec.get_common().get_streams().keySet()) {
        if (systemId(id)) {
          throw new InvalidTopologyException(id
              + " is not a valid component id");
        }
      }
    } else {
      throw new InvalidTopologyException("Unknow type component");
    }
  }

  private static void validateComponentInputs(Object obj)
      throws InvalidTopologyException {
    if (obj instanceof StateSpoutSpec) {
      StateSpoutSpec spec = (StateSpoutSpec) obj;
      if (!spec.get_common().get_inputs().isEmpty()) {
        throw new InvalidTopologyException("May not declare inputs for a spout");
      }
    }
    if (obj instanceof SpoutSpec) {
      SpoutSpec spec = (SpoutSpec) obj;
      if (!spec.get_common().get_inputs().isEmpty()) {
        throw new InvalidTopologyException("May not declare inputs for a spout");
      }
    }
  }
}