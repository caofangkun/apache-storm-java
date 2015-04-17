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

import java.io.IOException;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.zk.InprocessZookeeper;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.command.dev-zookeeper")
public class DevZookeeper {

  @SuppressWarnings("rawtypes")
  public static void main(String[] args) throws IOException,
      InterruptedException {

    Map conf = Utils.readStormConfig();
    String localpath =
        CoreUtil
            .parseString(conf.get(Config.DEV_ZOOKEEPER_PATH), "storm-local");

    CoreUtil.rmr(localpath);
    (new InprocessZookeeper(conf)).start();
  }
}
