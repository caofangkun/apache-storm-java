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
package org.apache.storm.zk;

import java.util.Map;
import java.util.Set;

import org.apache.storm.cluster.DistributedClusterState;
import org.apache.storm.command.OptionsProcessor;
import org.apache.storm.util.CoreUtil;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class ZkTool {
  public static final String READ_CMD = "read";
  public static final String LIST_CMD = "list";
  public static final String RMR_CMD = "rmr";

  public static void printUsage() {
    System.out.println("Usage:");
    System.out
        .println("   $STORM_HOME/bin/storm zktool -c [list|read] -p zk_path ");
  }

  public static String getData(DistributedClusterState zkClusterState,
      String path) throws Exception {
    byte[] data = zkClusterState.getData(path, false);
    if (data == null || data.length == 0) {
      return null;
    }
    return Utils.deserialize(data, String.class);
  }

  public static void readData(String path) {
    DistributedClusterState zkClusterState = null;
    try {
      conf.put(Config.STORM_ZOOKEEPER_ROOT, "/");
      zkClusterState = new DistributedClusterState(conf);
      String data = getData(zkClusterState, path);
      if (data == null) {
        System.out.println("No data of " + path);
        return;
      }
      StringBuilder sb = new StringBuilder();
      sb.append("Zk node " + path + "\n");
      sb.append("Readable data:" + data + "\n");
      System.out.println(sb.toString());
    } catch (Exception e) {
      if (zkClusterState == null) {
        System.err.println("Failed to connect ZK for: \n "
            + CoreUtil.stringifyError(e));
      } else {
        System.err.println("Failed to read data " + path + " for :\n"
            + CoreUtil.stringifyError(e));
      }
    } finally {
      if (zkClusterState != null) {
        zkClusterState.close();
      }
    }
  }

  public static void listPath(String path) {
    DistributedClusterState zkClusterState = null;
    try {
      // conf.put(Config.STORM_ZOOKEEPER_ROOT, "/");
      zkClusterState = new DistributedClusterState(conf);
      Set<String> paths = zkClusterState.getChildren(path, false);
      if (paths == null) {
        System.out.println("No children paths of " + path);
        return;
      }
      System.out.println("Zk node " + path + " children paths are:");
      for (String pt : paths) {
        System.out.println(pt);
      }
    } catch (Exception e) {
      if (zkClusterState == null) {
        System.err
            .println("Failed to connect ZK " + CoreUtil.stringifyError(e));
      } else {
        System.err.println("Failed to read data " + path + "\n"
            + CoreUtil.stringifyError(e));
      }
    } finally {
      if (zkClusterState != null) {
        zkClusterState.close();
      }
    }
  }

  public static void rmrPath(String path) {
    DistributedClusterState zkClusterState = null;
    try {
      zkClusterState = new DistributedClusterState(conf);
      zkClusterState.deleteNode(path);
    } catch (Exception e) {
      if (zkClusterState == null) {
        System.err
            .println("Failed to connect ZK " + CoreUtil.stringifyError(e));
      } else {
        System.err.println("Failed to delete path " + path + "\n"
            + CoreUtil.stringifyError(e));
      }
    } finally {
      if (zkClusterState != null) {
        zkClusterState.close();
      }
    }
  }

  private static Map conf;

  public static void main(String[] args) throws Exception {
    OptionsProcessor op = new OptionsProcessor();
    conf = Utils.readStormConfig();
    op.processArgs(conf, args);
    String command = (String) conf.get("zktool.command");
    String path = (String) conf.get("zktool.path");
    if (command == null || path == null) {
      System.out.println("Should input COMMAND name and PATH name");
      printUsage();
      return;
    }
    if (command.equalsIgnoreCase(READ_CMD)) {
      readData(path);
    }
    if (command.equalsIgnoreCase(LIST_CMD)) {
      listPath(path);
    }
    if (command.equalsIgnoreCase(RMR_CMD)) {
      rmrPath(path);
    }
  }
}
