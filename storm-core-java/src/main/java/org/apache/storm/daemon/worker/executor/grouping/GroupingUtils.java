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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.ClojureClass;

import backtype.storm.generated.Grouping;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Fields;

public class GroupingUtils {

/**
 * 
 * @param workerContext
 * @param componentId
 * @return map of stream id to component id to grouper
 */
  @ClojureClass(className = "backtype.storm.daemon.executor#outbound-components")
  public static Map<String, Map<String, MkGrouper>> outboundComponents(
      WorkerTopologyContext workerContext, String componentId) {
    Map<String, Map<String, MkGrouper>> stormIdToComponentIdToGrouper =
        new HashMap<String, Map<String, MkGrouper>>();

    Map<String, Map<String, Grouping>> outputGroupings =
        workerContext.getTargets(componentId);

    for (Entry<String, Map<String, Grouping>> entry : outputGroupings
        .entrySet()) {
      String streamId = entry.getKey();
      Map<String, Grouping> componentToGrouping = entry.getValue();
      Fields outFields =
          workerContext.getComponentOutputFields(componentId, streamId);
      Map<String, MkGrouper> componentGrouper =
          outboundGroupings(workerContext, componentId, streamId, outFields,
              componentToGrouping);
      stormIdToComponentIdToGrouper.put(streamId, componentGrouper);
    }
    return stormIdToComponentIdToGrouper;
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#outbound-groupings")
  private static Map<String, MkGrouper> outboundGroupings(
      WorkerTopologyContext workerContext, String thisComponentId,
      String streamId, Fields outFields,
      Map<String, Grouping> componentToGrouping) {
    Map<String, MkGrouper> componentGrouper = new HashMap<String, MkGrouper>();
    for (Entry<String, Grouping> cg : componentToGrouping.entrySet()) {
      String component = cg.getKey();
      List<Integer> componentTasks = workerContext.getComponentTasks(component);
      if (componentTasks != null && componentTasks.size() > 0) {
        Grouping tgrouping = cg.getValue();
        MkGrouper grouper =
            new MkGrouper(workerContext, thisComponentId, streamId, outFields,
                tgrouping, workerContext.getComponentTasks(component));
        componentGrouper.put(component, grouper);
      }
    }
    return componentGrouper;
  }
}
