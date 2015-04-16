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

import java.io.StringBufferInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;

import backtype.storm.generated.RebalanceOptions;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.command.rebalance")
public class rebalance {

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = { "--topologyName" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = "test";

  @Option(name = "--wait", aliases = { "-w" }, usage = "wait seconds default is 5")
  private int _wait = 5;

  @Option(name = "--num-workers", aliases = { "-n" }, usage = "num of workers default is 0 ")
  private int _numWorkers = 0;

  @Option(name = "--executor", aliases = { "-e" }, usage = "num of executors ,usage: -e split=10,count=20  ")
  private String _executor = null;

  private static void printUsage() {
    System.out.println("Usage:");
    System.out
        .println("    $STORM_HOME/bin/storm rebalance --name topoloygname -w waitsecs -n num_workers -e componedname=num_tasks,componedname2=num_tasks2");
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
      System.err.println();
      return;
    }

    if (_numWorkers <= 0) {
      throw new IllegalArgumentException("Need at least one worker");
    }
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException("name must be something");
    }

    String info = "Topology " + _name + " is rebalancing ";
    NimbusClient client = null;
    Map conf = Utils.readStormConfig();
    try {
      client = NimbusClient.getConfiguredClient(conf);
      RebalanceOptions options = new RebalanceOptions();
      options.set_wait_secs(_wait);
      info += " with delaySesc " + _wait + " ";
      options.set_num_workers(_numWorkers);
      info += " number of workers " + _numWorkers;
      if (_executor != null) {
        HashMap<String, Integer> executors = parseExecutor(_executor);
        options.set_num_executors(executors);
        info += " with executor " + executors.toString() + " ";
      }
      client.getClient().rebalance(_name, options);
      System.out.println(info);
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      printUsage();
    } finally {
      if (client != null) {
        client.close();
      }
    }

  }

  private static HashMap<String, Integer> parseExecutor(String str) {
    str = org.apache.commons.lang.StringUtils.deleteWhitespace(str);
    HashMap<String, Integer> executor = new HashMap<String, Integer>();
    String[] strs = str.split(",");
    Properties properties = new Properties();
    try {
      for (String s : strs) {
        properties.load(new StringBufferInputStream(s));
        for (final String name : properties.stringPropertyNames()) {
          Integer value = CoreUtil.parseInt(properties.getProperty(name));
          executor.put(name, value.intValue());
        }
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
    }
    return executor;
  };

  public static void main(String[] args) throws Exception {
    new rebalance().realMain(args);
  }
}
