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

import backtype.storm.ClojureClass;

import com.tencent.jstorm.daemon.worker.WorkerStatus;
import com.tencent.jstorm.daemon.worker.heartbeat.WorkerLocalHeartbeat;

@ClojureClass(className = "backtype.storm.daemon.supervisor#read-allocated-workers:[state hb]")
public class StateHeartbeat {
  private WorkerStatus state;
  private WorkerLocalHeartbeat hb;

  public StateHeartbeat(WorkerStatus state, WorkerLocalHeartbeat hb) {
    this.state = state;
    this.hb = hb;
  }

  public WorkerStatus getState() {
    return this.state;
  }

  public WorkerLocalHeartbeat getHeartbeat() {
    return this.hb;
  }

}
