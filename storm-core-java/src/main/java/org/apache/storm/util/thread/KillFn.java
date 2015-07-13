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

@ClojureClass(className = "backtype.storm.daemon.worker#mk-halting-timer#kill-fn")
public class KillFn extends RunnableCallback {
  private static final Logger LOG = LoggerFactory.getLogger(KillFn.class);
  private static final long serialVersionUID = 1L;
  private int status = 20;
  private String msg = "Error when processing an event";

  public KillFn() {
  }

  public KillFn(int status, String msg) {
    this.status = status;
    this.msg = msg;
  }

  @Override
  public <T> Object execute(T... args) {
    Throwable e = (Throwable) args[0];
    LOG.error(msg + CoreUtil.stringifyError(e));
    CoreUtil.exitProcess(status, msg);
    return e;
  }

  @Override
  public void run() {
    LOG.error(msg);
    CoreUtil.exitProcess(status, msg);
  }
}
