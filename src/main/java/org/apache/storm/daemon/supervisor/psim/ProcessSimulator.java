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
package org.apache.storm.daemon.supervisor.psim;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.ClojureClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.daemon.Shutdownable;

@ClojureClass(className = "backtype.storm.process_simulator")
public class ProcessSimulator {
  private static Logger LOG = LoggerFactory.getLogger(ProcessSimulator.class);

  @ClojureClass(className = "backtype.storm.process-simulator#kill-lock")
  protected final static Object killLock = new Object();

  @ClojureClass(className = "backtype.storm.process-simulator#process-map")
  protected final static ConcurrentHashMap<String, Shutdownable> processMap =
      new ConcurrentHashMap<String, Shutdownable>();

  @ClojureClass(className = "backtype.storm.process-simulator#register-process")
  public static void registerProcess(String pid, Shutdownable shutdownable) {
    processMap.put(pid, shutdownable);
  }

  @ClojureClass(className = "backtype.storm.process-simulator#process-handle")
  protected static Shutdownable getProcessHandle(String pid) {
    return processMap.get(pid);
  }

  public static Collection<Shutdownable> getAllProcessHandles() {
    return processMap.values();
  }

  @ClojureClass(className = "backtype.storm.process-simulator#kill-process")
  public static void killProcess(String pid) {
    // Uses `locking` in case cluster shuts down while supervisor is killing a
    // task
    synchronized (killLock) {
      LOG.info("killing process " + pid);
      Shutdownable shutdownable = getProcessHandle(pid);
      processMap.remove(pid);
      if (shutdownable != null) {
        shutdownable.shutdown();
      }
      LOG.info("Successfully killing process " + pid);
    }
  }

  @ClojureClass(className = "backtype.storm.process-simulator#kill-all-processes")
  public static void killAllProcesses() {
    Set<String> pids = processMap.keySet();
    for (String pid : pids) {
      killProcess(pid);
    }
    LOG.info("Successfully kill all processes");
  }
}
