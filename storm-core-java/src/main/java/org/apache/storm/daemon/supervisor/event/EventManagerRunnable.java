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

import java.io.InterruptedIOException;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.apache.storm.util.thread.RunnableCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ClojureClass(className = "backtype.storm.event#event-manager#runner#fn")
public class EventManagerRunnable implements Runnable {
  private static Logger LOG = LoggerFactory
      .getLogger(EventManagerRunnable.class);

  public EventManagerRunnable(EventManagerImp manager) {
    this.manager = manager;
  }

  EventManagerImp manager;
  Exception error = null;

  @Override
  public void run() {
    try {
      while (manager.isRunning()) {
        RunnableCallback r = null;
        r = manager.take();

        if (r == null) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {

          }
          continue;
        }

        r.run();
        Exception e = r.error();
        if (e != null) {
          throw e;
        }
        manager.proccessinc();
      }
    } catch (InterruptedIOException t) {
      LOG.error("Event manager interrupted while doing IO",
          CoreUtil.stringifyError(t));
    } catch (InterruptedException e) {
      error = e;
      LOG.error("Event Manager interrupted", CoreUtil.stringifyError(e));
    } catch (Throwable e) {
      LOG.error("Error when processing event ", CoreUtil.stringifyError(e));
      CoreUtil.exitProcess(20, "Error when processing an event");
    }
  }
}