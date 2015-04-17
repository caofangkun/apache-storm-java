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
package org.apache.storm.command;

import java.util.List;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.command.list")
public class ListTopologies {
  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("    $STORM_HOME/bin/storm list");
  }

  @SuppressWarnings("rawtypes")
  public static void main(String[] args) {
    NimbusClient client = null;
    try {
      Map conf = Utils.readStormConfig();
      client = NimbusClient.getConfiguredClient(conf);
      ClusterSummary clusterInfo = client.getClient().getClusterInfo();
      List<TopologySummary> topologies = clusterInfo.get_topologies();
      if (topologies == null || topologies.size() == 0) {
        System.out.println("No topologies running.");
        System.exit(0);
      }
      String msgFormat = "%-20s %-10s %-10s %-12s %-10s";
      System.out.println(String.format(msgFormat, "Topology_name", "Status",
          "Num_tasks", "Num_workers", "Uptime_secs"));
      System.out
          .println("-------------------------------------------------------------------");
      for (TopologySummary topology : topologies) {
        System.out.println(String.format(msgFormat, topology.get_name(),
            topology.get_status(), topology.get_num_tasks(),
            topology.get_num_workers(), topology.get_uptime_secs()));
      }
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      printUsage();
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
