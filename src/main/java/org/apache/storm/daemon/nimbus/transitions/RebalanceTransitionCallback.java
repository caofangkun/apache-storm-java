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

import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.nimbus.NimbusData;
import org.apache.storm.daemon.nimbus.NimbusUtils;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.BaseCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.RebalanceOptions;
import backtype.storm.generated.StormBase;
import backtype.storm.generated.TopologyActionOptions;
import backtype.storm.generated.TopologyStatus;

@ClojureClass(className = "backtype.storm.daemon.nimbus#rebalance-transition")
public class RebalanceTransitionCallback extends BaseCallback {
  private static Logger LOG = LoggerFactory
      .getLogger(RebalanceTransitionCallback.class);
  private NimbusData nimbusData;
  private String stormId;
  private TopologyStatus oldStatus;
  public static final int DEFAULT_DELAY_SECONDS = 60;

  public RebalanceTransitionCallback(NimbusData nimbusData, String stormId,
      TopologyStatus status) {
    this.nimbusData = nimbusData;
    this.stormId = stormId;
    this.oldStatus = status;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public <T> Object execute(T... args) {
    // time num-workers executor-overrides
    assert args.length == 3;

    Integer delaySecs = Integer.valueOf(String.valueOf(args[0]));
    Integer numWorkers = Integer.valueOf(String.valueOf(args[1]));
    Map<String, Integer> executorOverrides = (Map<String, Integer>) args[2];
    try {
      Map stormConf = NimbusUtils.readStormConf(nimbusData.getConf(), stormId);
      if (delaySecs == null) {
        delaySecs =
            CoreUtil.parseInt(
                stormConf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), 30);
      }
      DelayEvent delayEvent =
          new DelayEvent(nimbusData, stormId, delaySecs,
              StatusType.do_rebalance);
      delayEvent.execute(args);
    } catch (Exception e) {
      LOG.error("Failed Update state while rebalancing:"
          + CoreUtil.stringifyError(e));
    }

    StormBase stormBase = new StormBase();
    stormBase.set_status(TopologyStatus.REBALANCING);
    stormBase.set_prev_status(oldStatus);
    TopologyActionOptions topologyActionOptions = new TopologyActionOptions();
    RebalanceOptions rebalanceOptions = new RebalanceOptions();
    rebalanceOptions.set_wait_secs(delaySecs);
    rebalanceOptions.set_num_workers(numWorkers);
    rebalanceOptions.set_num_executors(executorOverrides);
    topologyActionOptions.set_rebalance_options(rebalanceOptions);
    stormBase.set_topology_action_options(topologyActionOptions);
    return stormBase;
  }
}
