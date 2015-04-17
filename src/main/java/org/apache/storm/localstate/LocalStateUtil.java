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
package org.apache.storm.localstate;

import org.apache.storm.ClojureClass;

import backtype.storm.generated.LSSupervisorId;
import backtype.storm.utils.LocalState;

@ClojureClass(className = "backtype.storm.local-state")
public class LocalStateUtil {
  
  @ClojureClass(className = "backtype.storm.local-state#LS-WORKER-HEARTBEAT")
  public static final String LS_WORKER_HEARTBEAT = "worker-heartbeat";
  @ClojureClass(className = "backtype.storm.local-state#LS-ID")
  public static final String LS_ID = "supervisor-id";
  @ClojureClass(className = "backtype.storm.local-state#LS-LOCAL-ASSIGNMENTS")
  public static final String LS_LOCAL_ASSIGNMENTS = "local-assignments";
  @ClojureClass(className = "backtype.storm.local-state#LS-APPROVED-WORKERS")
  public static final String LS_APPROVED_WORKERS = "approved-workers";
  
  
  @ClojureClass(className = "backtype.storm.local-state#ls-supervisor-id!")
  public static void lsSupervisorId (LocalState localState, String id) {
    localState.put(LS_ID, new LSSupervisorId(id));
  }
  
  //TODO

}
