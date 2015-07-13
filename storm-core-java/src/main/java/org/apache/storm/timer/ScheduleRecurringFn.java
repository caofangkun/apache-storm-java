package org.apache.storm.timer;

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
import org.apache.storm.ClojureClass;
import org.apache.storm.util.thread.RunnableCallback;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
@ClojureClass(className = "backtype.storm.timer#schedule-recurring#fn")
public class ScheduleRecurringFn extends RunnableCallback {

  private static final long serialVersionUID = 1L;
  private Timer timer;
  private int recurSecs;
  private RunnableCallback afn;

  public ScheduleRecurringFn(Timer timer, int recurSecs, RunnableCallback afn) {
    this.timer = timer;
    this.recurSecs = recurSecs;
    this.afn = afn;
  }

  @Override
  public void run() {
    afn.run();
    // this avoids a race condition with cancel-timer
    TimerUtil.schedule(timer, recurSecs, this, false);
  }
}
