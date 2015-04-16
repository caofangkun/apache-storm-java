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
package org.apache.storm.util.thread;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.storm.ClojureClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Time;

@ClojureClass(className = "backtype.storm.util#async-loop")
public class AsyncLoopThread implements SmartThread {
  private static final Logger LOG = LoggerFactory
      .getLogger(AsyncLoopThread.class);

  private Thread thread;
  public volatile boolean stop = false;

  // afn returns amount of time to sleep
  public AsyncLoopThread(RunnableCallback afn) {
    this.init(afn, false, Thread.NORM_PRIORITY, true);
  }

  public AsyncLoopThread(RunnableCallback afn, boolean daemon, int priority,
      boolean start) {
    this.init(afn, daemon, priority, start);
  }

  public AsyncLoopThread(RunnableCallback afn, boolean daemon,
      RunnableCallback kill_fn, int priority, boolean start) {
    this.init(afn, daemon, kill_fn, priority, start);
  }

  public AsyncLoopThread(RunnableCallback afn, boolean daemon,
      RunnableCallback kill_fn, int priority, boolean start, String threadName) {
    this.init(afn, daemon, kill_fn, priority, start, threadName);
  }

  public void init(RunnableCallback afn, boolean daemon, int priority,
      boolean start) {
    RunnableCallback kill_fn = new KillFn(1, "Async loop died!");
    this.init(afn, daemon, kill_fn, priority, start);
  }

  private void init(RunnableCallback afn, boolean daemon,
      RunnableCallback kill_fn, int priority, boolean start) {
    this.init(afn, daemon, kill_fn, priority, start, afn.getClass()
        .getSimpleName());

  }

  private void init(RunnableCallback afn, boolean daemon,
      RunnableCallback kill_fn, int priority, boolean start, String threadName) {
    Runnable runable = new AsyncLoopRunnable(afn, kill_fn);
    thread = new Thread(runable);
    thread.setName(threadName);
    thread.setDaemon(daemon);
    thread.setPriority(priority);
    thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        LOG.error("uncaughtException", e);
      }
    });
    if (start) {
      thread.start();
    }
  }

  @Override
  public void start() {
    thread.start();
  }

  @Override
  public void join() throws InterruptedException {
    thread.join();
  }

  // for test
  public void join(int times) throws InterruptedException {
    thread.join(times);
  }

  @Override
  public void interrupt() {
    thread.interrupt();
  }

  @Override
  public Boolean isSleeping() {
    return Time.isThreadWaiting(thread);
  }
}
