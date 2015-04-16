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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.storm.ClojureClass;
import org.apache.storm.thrift.Thrift;

import backtype.storm.generated.Grouping;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.daemon.executor#mk-grouper")
public class MkGrouper {
  private GroupingType grouptype;
  private IGrouper grouper;

  public MkGrouper(WorkerTopologyContext context, String componentId,
      String streamId, Fields outFields, Grouping thriftGrouping,
      List<Integer> targetTasks) {
    this.grouptype = Thrift.groupingType(thriftGrouping);
    init(thriftGrouping, outFields, context, componentId, streamId, targetTasks);
  }

  private void init(Grouping thriftGrouping, Fields outFields,
      WorkerTopologyContext context, String componentId, String streamId,
      List<Integer> targetTasks) {
    Collections.sort(targetTasks);
    switch (grouptype) {
    case fields:
      grouper = new FieldsGrouper(thriftGrouping, outFields, targetTasks);
      break;
    case all:
      grouper = new AllGrouper(targetTasks);
      break;
    case shuffle:
      grouper = new ShuffleGrouper(targetTasks);
      break;
    case local_or_shuffle:
      grouper = new LocalOrShuffleGrouper(context, targetTasks);
      break;
    case none:
      grouper = new NoneGrouper(targetTasks);
    case custom_obj:
      grouper =
          new CustomGrouper(Thrift.instantiateJavaObject(thriftGrouping
              .get_custom_object()), context, componentId, streamId,
              targetTasks);
      break;
    case custom_serialized:
      grouper =
          new CustomGrouper((CustomStreamGrouping) Utils.javaDeserialize(
              thriftGrouping.get_custom_serialized(), Serializable.class),
              context, componentId, streamId, targetTasks);
      break;
    default:
      grouper = new DefaultGrouper();
      break;
    }

  }

  /*
   * Returns a function that returns a vector of which task indices to send
   * tuple to, or just a single task index.
   */
  public List<Integer> grouper(Integer taskId, List<Object> values) {
    return grouper.fn(taskId, values);
  }

  public GroupingType getGrouptype() {
    return grouptype;
  }

  public void setGrouptype(GroupingType grouptype) {
    this.grouptype = grouptype;
  }

}
