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

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;

import backtype.storm.utils.Time;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
public class TimerUtil {

  @ClojureClass(className = "backtype.storm.timer#check-active!")
  private static void checkActive(Timer timer) {
    if (!timer.getActive().get()) {
      throw new IllegalStateException("Timer is not active");
    }
  }

  @ClojureClass(className = "backtype.storm.timer#schedule")
  public static void schedule(Timer timer, int delaySecs, RunnableCallback afn,
      Boolean checkActive) {
    if (checkActive) {
      checkActive(timer);
    }
    String id = CoreUtil.uuid();
    PriorityQueue<TimeObj> queue = timer.getQueue();
    synchronized (timer.getLock()) {
      Long secs =
          Time.currentTimeMillis() + CoreUtil.secsToMillisLong(delaySecs);
      TimeObj obj = new TimeObj(secs, afn, id);
      queue.add(obj);
    }
  }

  @ClojureClass(className = "backtype.storm.timer#schedule-recurring")
  public static void scheduleRecurring(Timer timer, int delaySecs,
      int recurSecs, RunnableCallback afn) {
    schedule(timer, delaySecs, new ScheduleRecurringFn(timer, recurSecs, afn),
        true);
  }

  @ClojureClass(className = "backtype.storm.timer#cancel-timer")
  public static void cancelTimer(Timer timer) throws InterruptedException {
    checkActive(timer);
    synchronized (timer.getLock()) {
      timer.getActive().set(false);
      timer.getTimerThread().interrupt();
    }
    timer.getCancelNotifier().acquire();
  }
  
  @ClojureClass(className = "backtype.storm.timer#timer-waiting?")
  public static boolean timerWaiting(Timer timer) throws InterruptedException {
    return Time.isThreadWaiting(timer.getTimerThread());
  }
}
