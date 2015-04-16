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
package org.apache.storm.daemon.nimbus.threads;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.nimbus.NimbusData;
import org.apache.storm.daemon.nimbus.NimbusUtils;
import org.apache.storm.daemon.nimbus.transitions.StatusType;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ClojureClass(className = "backtype.storm.daemon.nimbus#delay-event#transition")
public class DelayEventRunnable implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(DelayEventRunnable.class);

  private NimbusData data;
  private String topologyid;
  private StatusType status;

  public DelayEventRunnable(NimbusData data, String topologyid,
      StatusType status) {
    this.data = data;
    this.topologyid = topologyid;
    this.status = status;
  }

  @Override
  public void run() {
    try {
      NimbusUtils.transition(data, topologyid, false, status);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
  }
}
