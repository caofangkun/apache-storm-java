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

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.utils.Utils;

@ClojureClass(className = "backtype.storm.util#async-loop:thread")
public class AsyncLoopRunnable implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(AsyncLoopRunnable.class);

  private RunnableCallback afn;
  private RunnableCallback killfn;

  public AsyncLoopRunnable(RunnableCallback afn, RunnableCallback killfn) {
    this.afn = afn;
    this.killfn = killfn;
  }

  @Override
  public void run() {

    try {
      while (true) {
        Exception e = null;
        try {
          if (afn == null) {
            LOG.info("RunnableCallback afn is null");
            continue;
          }
          afn.run();
          e = afn.error();
        } catch (Exception ex) {
          e = ex;
        }
        if (e != null) {
          afn.shutdown();
          throw e;
        }
        Object rtn = afn.getResult();
        if (rtn != null) {
          long sleepTime = Long.parseLong(String.valueOf(rtn));
          CoreUtil.sleepSecs(sleepTime);
          if (sleepTime < 0) {
            // if sleeptime < 0 means should shutdown
            afn.shutdown();
            return;
          }
        }
      }
    } catch (Throwable t) {
      if (Utils.exceptionCauseIsInstanceOf(InterruptedException.class, t)) {
        LOG.info("Async loop interrupted!", t);
      } else {
        LOG.error("Async loop died!", t);
        killfn.execute(t);
      }
    }
  }
}