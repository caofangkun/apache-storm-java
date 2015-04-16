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

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import backtype.storm.Config;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class OptionsProcessor {
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;

  public OptionsProcessor() {

    // -t topologyname
    options.addOption(OptionBuilder.hasArg().withArgName("topologyname")
        .withLongOpt("topologyname")
        .withDescription("Specify the topologyname to submit").create('t'));

    // -w waitsecs
    options.addOption(OptionBuilder.hasArg().withArgName("waitsecs")
        .withLongOpt("waitsecs").withDescription("waitsecs").create('w'));

    // -n numworkers
    options.addOption(OptionBuilder.hasArg().withArgName("numworkers")
        .withLongOpt("numworkers").withDescription("numworkers").create('n'));

    // -v key=value -v key=value
    options
        .addOption(OptionBuilder
            .withValueSeparator()
            .hasArgs(2)
            .withArgName("key=value")
            .withLongOpt("var")
            .withDescription(
                "Variable subsitution to apply to Storm commands. e.g. -v A=B -v C=D")
            .create('v'));

    // -c command
    options.addOption(OptionBuilder.hasArg().withArgName("command")
        .withLongOpt("command").withDescription("command").create('c'));

    // -p path
    options.addOption(OptionBuilder.hasArg().withArgName("path")
        .withLongOpt("path").withDescription("path").create('p'));

  }

  public void processArgs(Map conf, String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);
      if (commandLine.hasOption('H')) {
        printUsage();
        return;
      }
      String topologyName = commandLine.getOptionValue("topologyname");
      conf.put(Config.TOPOLOGY_NAME, topologyName);

      String numWorkers = commandLine.getOptionValue("numworkers");
      conf.put("rebalance.topology.workers", numWorkers);

      String waitsecs = commandLine.getOptionValue("waitsecs");
      conf.put("delay.secs", waitsecs);

      String vars = commandLine.getOptionValue("var");
      conf.put("vars", vars);

      String command = commandLine.getOptionValue("command");
      conf.put("zktool.command", command);

      String path = commandLine.getOptionValue("path");
      conf.put("zktool.path", path);
    } catch (ParseException e) {
      e.printStackTrace();
    }

  }

  private void printUsage() {
    // TODO Auto-generated method stub
    System.out.println("Helping");
  }
}
