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
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Time;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
@ClojureClass(className = "backtype.storm.timer#mk-timer#timer-thread")
public class TimerThread extends Thread {
  private Logger LOG = LoggerFactory.getLogger(TimerThread.class);
  private AtomicBoolean active;
  private PriorityQueue<TimeObj> queue;
  private RunnableCallback killFn;
  private Object lock = new Object();
  private Semaphore notifier;

  public TimerThread(PriorityQueue<TimeObj> queue, RunnableCallback killFn,
      AtomicBoolean active, Semaphore notifier) {
    this.queue = queue;
    this.killFn = killFn;
    this.active = active;
    this.notifier = notifier;
  }

  @Override
  public void run() {
    try {
      if (active.get()) {
        synchronized (lock) {
          TimeObj elem = queue.peek();
          Long timeMillis = elem.getSecs();
          if (elem != null && (Time.currentTimeMillis() >= timeMillis)) {
            // It is imperative to not run the function inside the timer lock.
            // Otherwise, it is possible to deadlock if the fn deals with other
            // locks, like the submit lock.
            synchronized (lock) {
              RunnableCallback afn = queue.poll().getAfn();
              afn.run();
            }
          } else {
            if (timeMillis != null) {
              // ;; If any events are scheduled, sleep until
              // ;; event generation. If any recurring events
              // ;; are scheduled then we will always go
              // ;; through this branch, sleeping only the
              // ;; exact necessary amount of time.
              Time.sleep(timeMillis - Time.currentTimeMillis());
            } else {
              // ;; Otherwise poll to see if any new event
              // ;; was scheduled. This is, in essence, the
              // ;; response time for detecting any new event
              // ;; schedulings when there are no scheduled
              // ;; events.
              Time.sleep(1000);
            }
          }
        }
      }
    } catch (Throwable t) {
      // ;; Because the interrupted exception can be
      // ;; wrapped in a RuntimeException.
      if (!(t instanceof InterruptedException)) {
        killFn.run();
        active.set(false);
      }
      LOG.error(CoreUtil.stringifyError(t));
    }
    notifier.release();
  }

  public AtomicBoolean getActive() {
    return active;
  }

  public void setActive(AtomicBoolean active) {
    this.active = active;
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

  public Semaphore getNotifier() {
    return notifier;
  }

  public void setNotifier(Semaphore notifier) {
    this.notifier = notifier;
  }
}
