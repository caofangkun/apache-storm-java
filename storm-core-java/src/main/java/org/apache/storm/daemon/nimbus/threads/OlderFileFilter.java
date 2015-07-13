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
package org.apache.storm.daemon.nimbus.threads;

import java.io.File;
import java.io.FileFilter;

import org.apache.storm.ClojureClass;

@ClojureClass(className = "backtype.storm.daemon.nimbus#file-older-than?")
public class OlderFileFilter implements FileFilter {

  private int seconds;

  public OlderFileFilter(int seconds) {
    this.seconds = seconds;
  }

  /**
   * Filter the older file, skip the files' modify time which is less sec than
   * now
   */
  @Override
  public boolean accept(File pathname) {
    long current_time = System.currentTimeMillis();

    return pathname.isFile()
        && (pathname.lastModified() + seconds * 1000 <= current_time);
  }
}