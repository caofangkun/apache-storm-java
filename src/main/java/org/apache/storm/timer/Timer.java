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
package org.apache.storm.timer;

import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.thread.RunnableCallback;

import backtype.storm.utils.Time;

/**
 * The timer defined in this file is very similar to java.util.Timer, except it
 * integrates with Storm's time simulation capabilities. This lets us test code
 * that does asynchronous work on the timer thread
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
@ClojureClass(className = "backtype.storm.timer#mk-timer")
public class Timer {
  private PriorityQueue<TimeObj> queue;
  private AtomicBoolean active;
  private Object lock;
  private Semaphore cancelNotifier;
  private TimerThread timerThread;
  private String threadName;

  public Timer(RunnableCallback killFn, String timerName) {
    this.queue = new PriorityQueue<TimeObj>(10, new CompareSecs());
    this.active = new AtomicBoolean(true);
    this.lock = new Object();
    this.cancelNotifier = new Semaphore(0);
    this.threadName = timerName == null ? "timer" : timerName;
    timerThread = new TimerThread(queue, killFn, active, cancelNotifier);
    timerThread.setDaemon(true);
    timerThread.setName(threadName);
    timerThread.setPriority(Thread.MAX_PRIORITY);
    timerThread.start();
  }

  @ClojureClass(className = "backtype.storm.timer#timer-waiting?")
  private Boolean isTimerWaiting() {
    return Time.isThreadWaiting(timerThread);
  }

  public PriorityQueue<TimeObj> getQueue() {
    return queue;
  }

  public void setQueue(PriorityQueue<TimeObj> queue) {
    this.queue = queue;
  }

  public Object getLock() {
    return lock;
  }

  public void setLock(Object lock) {
    this.lock = lock;
  }

  public Semaphore getCancelNotifier() {
    return cancelNotifier;
  }

  public void setCancelNotifier(Semaphore cancelNotifier) {
    this.cancelNotifier = cancelNotifier;
  }

  public TimerThread getTimerThread() {
    return timerThread;
  }

  public void setTimerThread(TimerThread timerThread) {
    this.timerThread = timerThread;
  }

  public String getThreadName() {
    return threadName;
  }

  public void setThreadName(String threadName) {
    this.threadName = threadName;
  }

  public AtomicBoolean getActive() {
    return active;
  }

  public void setActive(AtomicBoolean active) {
    this.active = active;
  }
}