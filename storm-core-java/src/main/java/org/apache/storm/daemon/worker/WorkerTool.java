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
package org.apache.storm.daemon.worker;

import java.util.Map;

import org.apache.storm.config.ConfigUtil;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.daemon.worker.heartbeat.WorkerLocalHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;

public class WorkerTool {

  private static Logger LOG = LoggerFactory.getLogger(WorkerTool.class);

  public static void usage() {
    System.out.println("Usage: ");
    System.out.println("  bin/jstorm workertool workerid");
  }

  @SuppressWarnings("rawtypes")
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      LOG.info("Invalid parameter");
      usage();
      return;
    }
    String workerId = args[0];
    Map conf = Utils.readStormConfig();
    LocalState ls = ConfigUtil.workerState(conf, workerId);
    WorkerLocalHeartbeat whb =
        (WorkerLocalHeartbeat) ls.get(Common.LS_WORKER_HEARTBEAT);
    System.out.println(whb.toString());
  }
}
