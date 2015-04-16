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

import org.apache.storm.ClojureClass;
import org.apache.storm.guava.collect.Lists;
import org.apache.storm.thrift.Thrift;

import backtype.storm.generated.Grouping;
import backtype.storm.tuple.Fields;

@ClojureClass(className = "backtype.storm.daemon.executor#mk-fields-grouper")
public class FieldsGrouper implements IGrouper {

  private Grouping thriftGrouping;
  private Fields outFields;
  private List<Integer> targetTasks;
  private int numTasks;

  public FieldsGrouper(Grouping thriftGrouping, Fields outFields,
      List<Integer> targetTasks) {
    this.thriftGrouping = thriftGrouping;
    this.outFields = outFields;
    this.targetTasks = targetTasks;
    this.numTasks = targetTasks.size();
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-fields-grouper#fn")
  public List<Integer> fn(Integer taskId, List<Object> values) {
    if (Thrift.isGlobalGrouping(thriftGrouping)) {
      // It's possible for target to have multiple tasks if it reads
      // multiple sources
      return Lists.newArrayList(targetTasks.get(0));
    } else {
      Fields groupFields = new Fields(Thrift.fieldGrouping(thriftGrouping));
      List<Object> objs = outFields.select(groupFields, values);
      int hashCode = (objs.hashCode() & 0x7FFFFFFF);

      return Lists.newArrayList(targetTasks.get(hashCode % numTasks));
    }
  }
}
