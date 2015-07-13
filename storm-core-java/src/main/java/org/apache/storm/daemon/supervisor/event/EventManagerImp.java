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
package org.apache.storm.daemon.supervisor.event;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Time;

@ClojureClass(className = "backtype.storm.event#event-manager")
public class EventManagerImp implements EventManager {

  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(EventManagerImp.class);

  private AtomicInteger added = new AtomicInteger(0);
  private AtomicInteger processed = new AtomicInteger(0);
  private AtomicBoolean isRunning = new AtomicBoolean(true);
  private Thread runner;
  private LinkedBlockingQueue<RunnableCallback> queue =
      new LinkedBlockingQueue<RunnableCallback>();

  /**
   * Creates a thread to respond to events.Any error will cause process to halt
   * 
   * @param isDaemon
   */
  public EventManagerImp(boolean isDaemon) {
    Runnable runnerCallback = new EventManagerRunnable(this);
    this.runner = new Thread(runnerCallback);
    this.runner.setDaemon(isDaemon);
    this.runner.start();
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public RunnableCallback take() throws InterruptedException {
    RunnableCallback event = queue.take();// if null, block
    return event;
  }

  public RunnableCallback poll() throws InterruptedException {
    RunnableCallback event = queue.poll();// return null not block
    return event;
  }

  public void proccessinc() {
    processed.incrementAndGet();
  }

  @Override
  public void add(RunnableCallback eventFn) {
    // should keep track of total added and processed to know if this is
    // finished yet
    if (!this.isRunning()) {
      throw new RuntimeException(
          "Cannot add events to a shutdown event manager");
    }
    added.incrementAndGet();
    queue.add(eventFn);
  }

  @Override
  public boolean waiting() {
    return Time.isThreadWaiting(runner) || (processed.get() == added.get());
  }

  @Override
  public void shutdown() {
    isRunning.set(false);
    runner.interrupt();
    try {
      runner.join();
    } catch (InterruptedException e) {
      LOG.warn("InterruptedException:", e.getMessage());
    }
  }
}
