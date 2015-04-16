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
package org.apache.storm.daemon.nimbus;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.Map;

import org.apache.storm.ClojureClass;
import org.apache.storm.util.CoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.TimeCacheMap;

@ClojureClass(className = "backtype.storm.daemon.nimbus#file-cache-map")
public class FileCacheMap<K, V> extends TimeCacheMap implements
    TimeCacheMap.ExpiredCallback<K, V> {
  private final static Logger LOG = LoggerFactory.getLogger(FileCacheMap.class);

  public FileCacheMap(Map conf) {
    super(CoreUtil.parseInt(
        conf.get(Config.NIMBUS_FILE_COPY_EXPIRATION_SECS), 600));
  }

  @Override
  public void expire(K id, V stream) {
    try {
      if (stream != null) {
        if (stream instanceof Channel) {
          Channel channel = (Channel) stream;
          channel.close();
        } else if (stream instanceof BufferFileInputStream) {
          BufferFileInputStream is = (BufferFileInputStream) stream;
          is.close();
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }
  }
}
