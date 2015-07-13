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
package org.apache.storm.daemon.supervisor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.common.Common;
import org.apache.storm.util.CoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.generated.LSSupervisorId;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.utils.LocalState;
import clojure.lang.Atom;

@ClojureClass(className = "backtype.storm.daemon.supervisor#standalone-supervisor")
public class StandaloneSupervisor implements ISupervisor {
  private static final Logger LOG = LoggerFactory
      .getLogger(StandaloneSupervisor.class);

  private clojure.lang.Atom confAtom;
  private clojure.lang.Atom idAtom;
  private LocalState localState;
  private Map<Integer, Integer> virtualPortToPort;

  public StandaloneSupervisor() {
    this.confAtom = new Atom(null);
    this.idAtom = new Atom(null);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void prepare(Map conf, String localDir) {
    try {
      confAtom.reset(conf);
      localState = new LocalState(localDir);
      virtualPortToPort =
          (Map<Integer, Integer>) conf.get(CoreConfig.STORM_VIRTUAL_REAL_PORTS);
      LSSupervisorId lsSupervisorId =
          (LSSupervisorId) localState.get(Common.LS_ID);
      String currId = lsSupervisorId.get_supervisor_id();

      if (currId == null) {
        currId = SupervisorUtils.generateSupervisorId();
      }
      localState.put(Common.LS_ID, new LSSupervisorId(currId));
      idAtom.reset(currId);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }

  @Override
  public String getSupervisorId() {
    return (String) idAtom.deref();
  }

  @Override
  public String getAssignmentId() {
    return (String) idAtom.deref();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Object getMetadata() {
    Map conf = (Map) confAtom.deref();
    Set<Integer> ports = new HashSet<Integer>();
    List<Integer> portList =
        (List<Integer>) conf.get(Config.SUPERVISOR_SLOTS_PORTS);
    for (Integer svPort : portList) {
      if (virtualPortToPort.containsKey(svPort)) {
        ports.add(virtualPortToPort.get(svPort));
        LOG.info("Use {} ---> {}", svPort, virtualPortToPort.get(svPort));
      } else {
        ports.add(svPort);
        LOG.info("Keep {}", svPort);
      }
    }
    return ports;
  }

  @Override
  public boolean confirmAssigned(int port) {
    return true;
  }

  @Override
  public void killedWorker(int port) {

  }

  @Override
  public void assigned(Collection<Integer> ports) {

  }

}
