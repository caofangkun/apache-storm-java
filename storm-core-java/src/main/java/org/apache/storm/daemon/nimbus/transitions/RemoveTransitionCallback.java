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
package org.apache.storm.daemon.nimbus.transitions;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.nimbus.NimbusData;
import org.apache.storm.util.thread.BaseCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ClojureClass(className = "backtype.storm.daemon.nimbus#state-transitions#remove")
public class RemoveTransitionCallback extends BaseCallback {

  private static Logger LOG = LoggerFactory
      .getLogger(RemoveTransitionCallback.class);

  private NimbusData data;
  private String stormId;

  public RemoveTransitionCallback(NimbusData data, String stormId) {
    this.data = data;
    this.stormId = stormId;
  }

  @Override
  public <T> Object execute(T... args) {
    LOG.info("Killing topology: " + stormId);
    try {
      data.getStormClusterState().removeStorm(stormId);
    } catch (Exception e) {
      LOG.warn("Failed to remove StormBase " + stormId + " from ZK ", e);
    }
    return null;
  }

}
