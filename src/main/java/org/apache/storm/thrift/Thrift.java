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
package org.apache.storm.thrift;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.worker.executor.grouping.GroupingType;
import org.apache.storm.util.CoreUtil;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.clojure.RichShellBolt;
import backtype.storm.clojure.RichShellSpout;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.JavaObject;
import backtype.storm.generated.JavaObjectArg;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StormTopology._Fields;
import backtype.storm.generated.StreamInfo;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.thrift")
public class Thrift {
  private static Logger LOG = LoggerFactory.getLogger(Thrift.class);

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.thrift#instantiate-java-object")
  public static CustomStreamGrouping instantiateJavaObject(JavaObject obj) {

    List<JavaObjectArg> args = obj.get_args_list();
    Class[] paraTypes = new Class[args.size()];
    Object[] paraValues = new Object[args.size()];
    for (int i = 0; i < args.size(); i++) {
      JavaObjectArg arg = args.get(i);
      paraValues[i] = arg.getFieldValue();

      if (arg.getSetField().equals(JavaObjectArg._Fields.INT_ARG)) {
        paraTypes[i] = Integer.class;
      } else if (arg.getSetField().equals(JavaObjectArg._Fields.LONG_ARG)) {
        paraTypes[i] = Long.class;
      } else if (arg.getSetField().equals(JavaObjectArg._Fields.STRING_ARG)) {
        paraTypes[i] = String.class;
      } else if (arg.getSetField().equals(JavaObjectArg._Fields.BOOL_ARG)) {
        paraTypes[i] = Boolean.class;
      } else if (arg.getSetField().equals(JavaObjectArg._Fields.BINARY_ARG)) {
        paraTypes[i] = ByteBuffer.class;
      } else if (arg.getSetField().equals(JavaObjectArg._Fields.DOUBLE_ARG)) {
        paraTypes[i] = Double.class;
      } else {
        paraTypes[i] = Object.class;
      }
    }

    try {
      Class clas = Class.forName(obj.get_full_class_name());
      Constructor cons = clas.getConstructor(paraTypes);
      return (CustomStreamGrouping) cons.newInstance(paraValues);
    } catch (Exception e) {
      LOG.error("instantiate_java_object fail", e);
    }

    return null;

  }

  @ClojureClass(className = "backtype.storm.thrift#grouping-constants")
  public static Map<Grouping._Fields, GroupingType> groupingConstants =
      new EnumMap<Grouping._Fields, GroupingType>(Grouping._Fields.class);
  static {
    groupingConstants.put(Grouping._Fields.FIELDS, GroupingType.fields);
    groupingConstants.put(Grouping._Fields.SHUFFLE, GroupingType.shuffle);
    groupingConstants.put(Grouping._Fields.ALL, GroupingType.all);
    groupingConstants.put(Grouping._Fields.NONE, GroupingType.none);
    groupingConstants.put(Grouping._Fields.CUSTOM_SERIALIZED,
        GroupingType.custom_serialized);
    groupingConstants.put(Grouping._Fields.CUSTOM_OBJECT,
        GroupingType.custom_obj);
    groupingConstants.put(Grouping._Fields.DIRECT, GroupingType.direct);
    groupingConstants.put(Grouping._Fields.LOCAL_OR_SHUFFLE,
        GroupingType.local_or_shuffle);
  }

  @ClojureClass(className = "backtype.storm.thrift#grouping-type")
  public static GroupingType groupingType(Grouping grouping) {
    Grouping._Fields fields = grouping.getSetField();
    return groupingConstants.get(fields);
  }

  @ClojureClass(className = "backtype.storm.thrift#field-grouping")
  public static List<String> fieldGrouping(Grouping grouping) {
    if (!groupingType(grouping).equals(GroupingType.fields)) {
      throw new IllegalArgumentException(
          "Tried to get grouping fields from non fields grouping");
    }
    return grouping.get_fields();
  }

  @ClojureClass(className = "backtype.storm.thrift#global-grouping?")
  public static boolean isGlobalGrouping(Grouping grouping) {
    if (groupingType(grouping).equals(GroupingType.fields)) {
      return fieldGrouping(grouping).isEmpty();
    }
    return false;
  }

  @ClojureClass(className = "backtype.storm.thrift#parallelism-hint")
  public static int parallelismHint(ComponentCommon component_common) {
    int phint = component_common.get_parallelism_hint();
    if (!component_common.is_set_parallelism_hint()) {
      phint = 1;
    }
    return phint;
  }

  @ClojureClass(className = "backtype.storm.thrift#nimbus-client-and-conn")
  public static Object[] nimbusClientAndConn(String host, Integer port)
      throws TTransportException {
    LOG.info("Connecting to Nimbus at " + host + ":" + port);
    TFramedTransport transport = new TFramedTransport(new TSocket(host, port));
    TBinaryProtocol prot = new TBinaryProtocol(transport);
    Nimbus.Client client = new Nimbus.Client(prot);
    transport.open();
    Object[] rtn = { client, transport };
    return rtn;
  }

  @ClojureClass(className = "backtype.storm.thrift#direct-output-fields")
  public static StreamInfo directOutputFields(List<String> fields) {
    return new StreamInfo(fields, true);
  }

  @ClojureClass(className = "backtype.storm.thrift#output-fields")
  public static StreamInfo outputFields(List<String> fields) {
    return new StreamInfo(fields, false);
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.thrift#mk-output-spec")
  public static Map<String, Object> mkOutputSpec(Object outputSpec) {

    Map<String, Object> output_spec;
    if (outputSpec instanceof Map) {
      output_spec = (Map<String, Object>) outputSpec;
    } else {
      output_spec = new HashMap<String, Object>();
      output_spec.put(Utils.DEFAULT_STREAM_ID, outputSpec);
    }

    for (Map.Entry<String, Object> e : output_spec.entrySet()) {
      String key = e.getKey();
      Object value = e.getValue();
      if (!(value instanceof StreamInfo)) {
        StreamInfo info = new StreamInfo((List<String>) value, false);
        output_spec.put(key, info);
      }
    }
    return output_spec;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.thrift#mk-spout-spec*")
  public static SpoutSpec mkSpoutSpec(ISpout spout,
      Map<String, StreamInfo> outputs, Integer parallelism_hint, Map conf) {
    ComponentObject spout_object =
        ComponentObject.serialized_java(Utils.serialize(spout));
    ComponentCommon common =
        mkPlainComponentCommon(null, outputs, parallelism_hint, conf);
    return new SpoutSpec(spout_object, common);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.thrift#mk-bolt-spec*")
  public static Bolt mkBoltSpec(Map<Object, Object> inputs, Bolt bolt,
      Map<String, StreamInfo> output_spec, Integer parallelism_hint, Map conf) {
    ComponentCommon common =
        mkPlainComponentCommon(mkInputs(inputs), output_spec, parallelism_hint,
            conf);
    return new Bolt(ComponentObject.serialized_java(Utils.serialize(bolt)),
        common);
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-shuffle-grouping")
  public static Grouping mkShuffleGrouping() {
    return Grouping.shuffle(new NullStruct());
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-local-or-shuffle-grouping")
  public static Grouping mkLocalOrShuffleGrouping() {
    return Grouping.local_or_shuffle(new NullStruct());
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-fields-grouping")
  public static Grouping mkFieldsGrouping(List<String> fields) {
    return Grouping.fields(fields);
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-global-grouping")
  public static Grouping mkGlobalGrouping() {
    return mkFieldsGrouping(new ArrayList<String>());
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-direct-grouping")
  public static Grouping mkDirectGrouping() {
    return Grouping.direct(new NullStruct());
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-all-grouping")
  public static Grouping mkAllGrouping() {
    return Grouping.all(new NullStruct());
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-none-grouping")
  public static Grouping mkNoneGrouping() {
    return Grouping.none(new NullStruct());
  }

  @ClojureClass(className = "backtype.storm.thrift#deserialized-component-object")
  public static Object deserializedComponentObject(ComponentObject obj) {
    if (!(obj.getSetField().equals(ComponentObject._Fields.SERIALIZED_JAVA))) {
      throw new RuntimeException(
          "Cannot deserialize non-java-serialized object");
    }
    return Utils.javaDeserialize(obj.get_serialized_java(), Serializable.class);
  }

  @ClojureClass(className = "backtype.storm.thrift#serialize-component-object")
  public static ComponentObject serializeComponentObject(Object obj) {
    return ComponentObject.serialized_java(Utils.serialize(obj));
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.thrift#mk-plain-component-common")
  public static ComponentCommon mkPlainComponentCommon(
      Map<GlobalStreamId, Grouping> inputs,
      Map<String, StreamInfo> output_spec, Integer parallelism_hint, Map conf) {
    ComponentCommon ret = new ComponentCommon(inputs, output_spec);
    if (parallelism_hint != null) {
      ret.set_parallelism_hint(parallelism_hint);
    }
    if (conf != null) {
      ret.set_json_conf(CoreUtil.to_json(conf));
    }
    return ret;
  }

  @SuppressWarnings("rawtypes")
  public static Bolt mkBoltSpec(Map<GlobalStreamId, Grouping> inputs,
      IBolt bolt, Map<String, StreamInfo> output, Integer p, Map conf) {
    ComponentCommon common = mkPlainComponentCommon(inputs, output, p, conf);
    ComponentObject component =
        ComponentObject.serialized_java(Utils.serialize(bolt));
    return new Bolt(component, common);
  }

  @ClojureClass(className = "backtype.storm.thrift#mk-topology")
  public static StormTopology mkTopology(Map<String, InternalSpoutSpec> spouts,
      Map<String, InternalBoltSpec> bolts) {
    TopologyBuilder tb = new TopologyBuilder();
    // spout
    for (Map.Entry<String, InternalSpoutSpec> se : spouts.entrySet()) {
      String spoutId = se.getKey();
      InternalSpoutSpec iss = se.getValue();
      SpoutDeclarer sd =
          tb.setSpout(spoutId, (IRichSpout) iss.getSpout(),
              iss.getParallelismHint());
      if (null != iss.getConf()) {
        sd.addConfigurations(iss.getConf());
      }
    }
    // bolt
    for (Map.Entry<String, InternalBoltSpec> be : bolts.entrySet()) {
      String boltId = be.getKey();
      InternalBoltSpec ibs = be.getValue();
      BoltDeclarer bd = null;
      Object tmpBolt = ibs.getBolt();
      if (tmpBolt instanceof IRichBolt) {
        bd =
            tb.setBolt(boltId, (IRichBolt) ibs.getBolt(),
                ibs.getParallelismHint());
      } else if (tmpBolt instanceof IBasicBolt) {
        bd =
            tb.setBolt(boltId, (IBasicBolt) ibs.getBolt(),
                ibs.getParallelismHint());
      }

      if (null != ibs.getConf()) {
        bd.addConfigurations(ibs.getConf());
      }

      if (null != ibs.getInputs()) {
        addInputs(bd, ibs.getInputs());
      }
    }
    return tb.createTopology();
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.thrift#mk-spout-spec")
  public static InternalSpoutSpec mkInternalSpoutSpec(Object spout,
      Integer parallelismHint, Integer p, Map conf) {
    if (null != p) {
      parallelismHint = p;
    }
    return (new Thrift()).new InternalSpoutSpec(parallelismHint, conf, spout);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.thrift#mk-bolt-spec")
  public static InternalBoltSpec mkInternalBoltSpec(Map<Object, Object> inputs,
      Object bolt, Integer parallelismHint, Integer p, Map conf) {
    if (null != p) {
      parallelismHint = p;
    }
    return (new Thrift()).new InternalBoltSpec(inputs, parallelismHint, conf,
        bolt);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.thrift#mk-shell-bolt-spec")
  public static InternalBoltSpec mkShellBoltSpec(Map<Object, Object> inputs,
      String[] command, Map<String, StreamInfo> output_spec,
      Integer parallelism_hint, Integer p, Map conf) {

    return mkInternalBoltSpec(inputs, new RichShellBolt(command, output_spec),
        parallelism_hint, p, conf);
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.thrift#mk-shell-bolt-spec")
  public static InternalSpoutSpec mkShellSpoutSpec(String[] command,
      Map<String, StreamInfo> output_spec, Integer parallelism_hint, Integer p,
      Map conf) {

    return mkInternalSpoutSpec(new RichShellSpout(command, output_spec),
        parallelism_hint, p, conf);
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.thrift#mk-inputs")
  private static Map<GlobalStreamId, Grouping> mkInputs(
      Map<Object, Object> inputs) {
    if (null != inputs) {
      Map<GlobalStreamId, Grouping> result =
          new HashMap<GlobalStreamId, Grouping>(inputs.size());
      for (Map.Entry<Object, Object> entry : inputs.entrySet()) {
        Object streamId = entry.getKey();
        GlobalStreamId tmpStream = null;
        if (streamId instanceof List && ((List<String>) streamId).size() > 2) {
          ((List<String>) streamId).get(0);
          tmpStream =
              new GlobalStreamId(((List<String>) streamId).get(0),
                  ((List<String>) streamId).get(1));
        } else {
          tmpStream =
              new GlobalStreamId((String) streamId, Utils.DEFAULT_STREAM_ID);
        }

        result.put(tmpStream, mkGrouping(entry.getValue()));
      }
      return result;
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.thrift#add-inputs")
  private static void addInputs(BoltDeclarer declarer,
      Map<Object, Object> inputs) {
    Map<GlobalStreamId, Grouping> tmpInputs = mkInputs(inputs);
    if (null != tmpInputs) {
      for (Map.Entry<GlobalStreamId, Grouping> entry : tmpInputs.entrySet()) {
        declarer.grouping(entry.getKey(), entry.getValue());
      }
    }
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "backtype.storm.thrift#mk-grouping")
  private static Grouping mkGrouping(Object groupingSpec) {
    if (null == groupingSpec) {
      return mkNoneGrouping();
    } else if (groupingSpec instanceof Grouping) {
      return (Grouping) groupingSpec;
    } else if (groupingSpec instanceof CustomStreamGrouping) {
      return Grouping.custom_serialized(Utils.serialize(groupingSpec));
    } else if (groupingSpec instanceof JavaObject) {
      return Grouping.custom_object((JavaObject) groupingSpec);
    } else if (groupingSpec instanceof List) {
      return mkFieldsGrouping((List<String>) groupingSpec);
    } else if (groupingSpec instanceof InternalGroupingType) {
      InternalGroupingType type = (InternalGroupingType) groupingSpec;
      Grouping result = null;
      switch (type) {
      case ALL:
        result = mkAllGrouping();
        break;
      case DIRECT:
        result = mkDirectGrouping();
        break;
      case SHUFFLE:
        result = mkShuffleGrouping();
        break;
      case GLOBAL:
        result = mkGlobalGrouping();
        break;
      case LOCAL_OR_SHUFFLE:
        result = mkLocalOrShuffleGrouping();
        break;
      case NONE:
        result = mkNoneGrouping();
        break;
      }
      return result;
    } else {
      throw new IllegalArgumentException(groupingSpec
          + " is not a valid grouping");
    }
  }

  public static Set<_Fields> STORM_TOPOLOGY_FIELDS = StormTopology.metaDataMap
      .keySet();
  public static StormTopology._Fields[] SPOUT_FIELDS = {
      StormTopology._Fields.SPOUTS, StormTopology._Fields.STATE_SPOUTS };

  @SuppressWarnings("rawtypes")
  class InternalBaseComponent {
    private Integer parallelismHint;

    private Map conf;

    public InternalBaseComponent(Integer parallelismHint, Map conf) {
      this.parallelismHint = parallelismHint;
      this.conf = conf;
    }

    public Integer getParallelismHint() {
      return parallelismHint;
    }

    public void setParallelismHint(Integer parallelismHint) {
      this.parallelismHint = parallelismHint;
    }

    public Map getConf() {
      return conf;
    }

    public void setConf(Map conf) {
      this.conf = conf;
    }

  }

  public class InternalSpoutSpec extends InternalBaseComponent {
    private Object spout;

    @SuppressWarnings("rawtypes")
    public InternalSpoutSpec(Integer parallelismHint, Map conf, Object spout) {
      super(parallelismHint, conf);
      this.spout = spout;
    }

    public Object getSpout() {
      return spout;
    }

    public void setSpout(Object spout) {
      this.spout = spout;
    }

  }

  public class InternalBoltSpec extends InternalBaseComponent {

    private Object bolt;
    private Map<Object, Object> inputs;

    @SuppressWarnings("rawtypes")
    public InternalBoltSpec(Map<Object, Object> inputs,
        Integer parallelismHint, Map conf, Object bolt) {
      super(parallelismHint, conf);
      this.bolt = bolt;
      this.inputs = inputs;
    }

    public Object getBolt() {
      return bolt;
    }

    public void setBolt(Object bolt) {
      this.bolt = bolt;
    }

    public Map<Object, Object> getInputs() {
      return inputs;
    }

    public void setInputs(Map<Object, Object> inputs) {
      this.inputs = inputs;
    }

  }

  public enum InternalGroupingType {
    SHUFFLE, LOCAL_OR_SHUFFLE, NONE, ALL, GLOBAL, DIRECT;
  }
}
