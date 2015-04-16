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

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.apache.storm.util.CoreUtil;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class shellSubmission {
  
  @SuppressWarnings("rawtypes")
  public static void main(String[] args) {
    if (args == null || args.length == 0) {
      System.err.println("Should input resourcesdir");
      System.exit(1);
    }
    try {
      Map conf = Utils.readStormConfig();
      Random random = new Random();
      String tmpjarpath = "stormshell" + random.nextInt(10000000) + ".jar";
      String resourcesdir = args[0];
      CoreUtil.execCommand("jar cf " + tmpjarpath + " " + resourcesdir);

      String host = (String) conf.get(Config.NIMBUS_HOST);
      String port = (String) conf.get(Config.NIMBUS_THRIFT_PORT);
      StormSubmitter.submitJar(conf, tmpjarpath);
      String command =
          Arrays.toString(args) + " " + host + " " + port + " " + tmpjarpath;
      CoreUtil.execCommand(command);
      CoreUtil.execCommand("rm " + tmpjarpath);
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      throw new RuntimeException(e);
    }
  }
}
