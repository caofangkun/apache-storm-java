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

import java.util.Map;

import org.apache.storm.util.CoreUtil;

import backtype.storm.generated.KillOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class kill_topology {
  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("    $STORM_HOME/bin/storm kill topology_name");
  }

  public static void main(String[] args) {
    if (args == null || args.length == 0) {
      System.out.println("Should privide TOPOLOGY_NAME at least!");
      printUsage();
      return;
    }
    String topologyName = args[0];
    NimbusClient client = null;
    try {
      Map conf = Utils.readStormConfig();
      client = NimbusClient.getConfiguredClient(conf);
      if (args.length == 1) {
        client.getClient().killTopology(topologyName);
      } else {
        int delaySeconds = CoreUtil.parseInt(args[1], 3);
        KillOptions options = new KillOptions();
        options.set_wait_secs(delaySeconds);
        client.getClient().killTopologyWithOpts(topologyName, options);
      }
      System.out.println("Successfully submit command kill " + topologyName);
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
