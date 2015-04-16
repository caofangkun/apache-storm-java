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
package org.apache.storm.daemon.supervisor.sync;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.ClojureClass;
import org.apache.storm.daemon.supervisor.event.EventManager;
import org.apache.storm.util.thread.RunnableCallback;

@ClojureClass(className = "backtype.storm.daemon.supervisor#mk-supervisor#schedule-recurring#fn")
public class AddEventFn extends RunnableCallback {
  private static final long serialVersionUID = 1L;
  private EventManager eventManager;
  private RunnableCallback event;
  private AtomicBoolean active;
  private Integer result;
  private int frequence;

  public AddEventFn(EventManager eventManager, RunnableCallback event,
      AtomicBoolean active, int frequence) {
    this.eventManager = eventManager;
    this.event = event;
    this.active = active;
    this.result = null;
    this.frequence = frequence;
  }

  @Override
  public void run() {
    eventManager.add(event);
    if (active.get()) {
      this.result = frequence;
    } else {
      this.result = -1;
    }
  }

  @Override
  public Object getResult() {
    return result;
  }
}
