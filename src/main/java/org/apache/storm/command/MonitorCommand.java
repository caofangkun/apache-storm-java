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

import org.apache.storm.ClojureClass;

import backtype.storm.utils.Monitor;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * """Syntax: [storm monitor topology-name [-i interval-secs] [-m component-id]
 * [-s stream-id] [-w [emitted | transferred]]]
 * 
 * Monitor given topology's throughput interactively. One can specify
 * poll-interval, component-id, stream-id, watch-item[emitted | transferred] By
 * default, poll-interval is 4 seconds; all component-ids will be list;
 * stream-id is 'default'; watch-item is 'emitted';
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.command.monitor")
public class MonitorCommand {
  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = { "--topologyName" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = null;

  @Option(name = "--interval", aliases = { "-i" }, usage = "interval secs default 4")
  private int _interval = 4;

  @Option(name = "--component", aliases = { "-m" }, usage = "component default null")
  private String _component = null;

  @Option(name = "--stream", aliases = { "-s" }, usage = "stream default null")
  private String _stream = null;

  @Option(name = "--watch", aliases = { "-w" }, usage = "watch default \"demitted\"")
  private String _watch = null;

  private static void printUsage() {
    System.out.println("Usage:");
    System.out
        .println("    $STORM_HOME/bin/storm storm monitor topology-name [-i interval-secs] [-m component-id] [-s stream-id] [-w [emitted | transferred]]");
    System.out.println("Monitor given topology's throughput interactively.");
    System.out
        .println("One can specify poll-interval, component-id, stream-id, watch-item[emitted | transferred]");
    System.out.println("By default,");
    System.out.println("    poll-interval is 4 seconds;");
    System.out.println("    all component-ids will be list;");
    System.out.println("    stream-id is 'default';");
    System.out.println("    watch-item is 'emitted';");
  }

  @SuppressWarnings("rawtypes")
  public void realMain(String[] args) throws Exception {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      _help = true;
    }
    if (_help) {
      parser.printUsage(System.err);
      printUsage();
      System.err.println();
      return;
    }

    Monitor mon = new Monitor();
    mon.set_interval(_interval);

    if (_name != null) {
      mon.set_topology(_name);
    }

    if (_component != null) {
      mon.set_component(_component);
    }

    if (_stream != null) {
      mon.set_stream(_stream);
    }

    if (_watch != null) {
      mon.set_watch(_watch);
    }

    NimbusClient nimbus = null;
    Map conf = Utils.readStormConfig();
    try {
      nimbus = NimbusClient.getConfiguredClient(conf);

      mon.metrics(nimbus.getClient());
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    MonitorCommand monCommand = new MonitorCommand();
    monCommand.realMain(args);
  }

}
