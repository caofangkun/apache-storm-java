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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.ClojureClass;
import backtype.storm.generated.StormBase;
import backtype.storm.generated.TopologyStatus;

import com.tencent.jstorm.daemon.nimbus.NimbusData;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.utils.thread.Callback;

@ClojureClass(className = "backtype.storm.daemon.nimbus#state-transitions")
public class StateTransitions {

  private final static Logger LOG = LoggerFactory
      .getLogger(StateTransitions.class);

  private NimbusData data;
  private Map<String, Object> topologyLocks =
      new ConcurrentHashMap<String, Object>();

  public StateTransitions(NimbusData data) {
    this.data = data;

  }

  public <T> void transition(String stormId, StatusType changeStatus, T... args)
      throws Exception {
    transition(stormId, false, changeStatus, args);
  }

  public <T> void transition(String stormId, boolean errorOnNoTransition,
      StatusType changeStatusType, T... args) throws Exception {
    // lock outside
    Object lock = topologyLocks.get(stormId);
    if (lock == null) {
      lock = new Object();
      topologyLocks.put(stormId, lock);
    }

    synchronized (lock) {
      transitionLock(stormId, errorOnNoTransition, changeStatusType, args);
      // update the lock times
      topologyLocks.put(stormId, lock);
    }
  }

  private StormBase topologyStatus(String stormId) throws Exception {
    StormBase stormbase = data.getStormClusterState().stormBase(stormId, null);
    if (stormbase == null) {
      return null;
    } else {
      return stormbase;
    }
  }

  public <T> void transitionLock(String stormId, boolean errorOnNoTransition,
      StatusType changeStatusType, T... args) throws Exception {
    StormBase stormBase = topologyStatus(stormId);
    // handles the case where event was scheduled but topology has been
    // removed
    if (stormBase == null) {
      LOG.info("Cannot apply event {} to {} because topology no longer exists",
          changeStatusType, stormId);
      return;
    }
    TopologyStatus status = stormBase.get_status();
    
    Map<TopologyStatus, Map<StatusType, Callback>> callbackMap =
        stateTransitions(stormId, status, stormBase);

    // get current changingCallbacks
    Map<StatusType, Callback> changingCallbacks =
        callbackMap.get(stormBase.get_status());

    if (changingCallbacks == null
        || changingCallbacks.containsKey(changeStatusType) == false
        || changingCallbacks.get(changeStatusType) == null) {
      String msg =
          "No transition for event: changing status "
              + changeStatusType.getStatus() + ", current status: "
              + stormBase.get_status() + " storm-id: " + stormId;
      LOG.info(msg);
      if (errorOnNoTransition) {
        throw new RuntimeException(msg);
      }
      return;
    }

    Callback callback = changingCallbacks.get(changeStatusType);

    Object obj = callback.execute(args);
    if (obj != null && obj instanceof StormBase) {
      StormBase newStormStatus = (StormBase) obj;
      NimbusUtils.setTopologyStatus(data, stormId, newStormStatus);
    }
    return;
  }

  @ClojureClass(className = "backtype.storm.daemon.nimbus#state-transitions")
  private Map<TopologyStatus, Map<StatusType, Callback>> stateTransitions(
      String stormId, TopologyStatus status, StormBase stormBase) {

    Map<TopologyStatus, Map<StatusType, Callback>> rtn =
        new HashMap<TopologyStatus, Map<StatusType, Callback>>();

    // current status type is active
    Map<StatusType, Callback> activeMap = new HashMap<StatusType, Callback>();
    activeMap.put(StatusType.inactivate, new InactiveTransitionCallback());
    activeMap.put(StatusType.activate, null);
    activeMap.put(StatusType.rebalance, new RebalanceTransitionCallback(data,
        stormId, status));
    activeMap.put(StatusType.kill, new KillTransitionCallback(data, stormId));
    rtn.put(TopologyStatus.ACTIVE, activeMap);

    // current status type is inactive
    Map<StatusType, Callback> inactiveMap = new HashMap<StatusType, Callback>();
    inactiveMap.put(StatusType.activate, new ActiveTransitionCallback());
    inactiveMap.put(StatusType.inactivate, null);
    inactiveMap.put(StatusType.rebalance, new RebalanceTransitionCallback(data,
        stormId, status));
    inactiveMap.put(StatusType.kill, new KillTransitionCallback(data, stormId));
    rtn.put(TopologyStatus.INACTIVE, inactiveMap);

    // current status type is killed
    Map<StatusType, Callback> killedMap = new HashMap<StatusType, Callback>();
    int killDelaySecs =
        stormBase.get_topology_action_options().get_kill_options()
            .get_wait_secs();
    killedMap.put(StatusType.startup, new DelayEvent(data, stormId,
        killDelaySecs, StatusType.remove));
    killedMap.put(StatusType.kill, new KillTransitionCallback(data, stormId));
    killedMap.put(StatusType.remove,
        new RemoveTransitionCallback(data, stormId));
    rtn.put(TopologyStatus.KILLED, killedMap);

    // current status type is rebalancing
    Map<StatusType, Callback> rebalancingMap =
        new HashMap<StatusType, Callback>();
    int rebalanceDelaySecs =
        stormBase.get_topology_action_options().get_rebalance_options()
            .get_wait_secs();
    rebalancingMap.put(StatusType.startup, new DelayEvent(data, stormId,
        rebalanceDelaySecs, StatusType.do_rebalance));
    rebalancingMap.put(StatusType.kill, new KillTransitionCallback(data,
        stormId));
    rebalancingMap.put(StatusType.do_rebalance,
        new DoRebalanceTransitionCallback(data, stormId, status, stormBase));
    rtn.put(TopologyStatus.REBALANCING, rebalancingMap);

    return rtn;

  }
}
