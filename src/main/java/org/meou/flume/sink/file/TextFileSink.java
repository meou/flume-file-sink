/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.meou.flume.sink.file;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(TextFileSink.class);
  private static final String DEFAULT_FILENAME = "/tmp/TextFileSink.log";
  private static String filename = null;

  public void configure(Context context) {
    filename = context.getString("filename",DEFAULT_FILENAME);
    logger.debug("filename="+filename);
  }

  @Override
  public void start() {
    super.start();
    logger.info("TextFileSink started");
  }

  @Override
  public void stop() {
    logger.info("TextFileSink started");
    super.stop();    
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    try {
      transaction.begin();
      Event event = channel.take();
      if (event != null) {
        String body = new String(event.getBody());
        Map<String, String> headers = event.getHeaders();
        BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true));
        StringBuffer sb = new StringBuffer();

        if ((headers == null) || headers.isEmpty()) {
          logger.debug("Event headers empty.");
        } else {
          for (Map.Entry<String, String> entry: headers.entrySet()) {
            sb.append(entry.getKey() + ":" + entry.getValue() + "\n");
          }
          sb.append("\n");
        }
        if ((body == null) || (body.length() == 0)) {
          logger.debug("Event body empty.");
        } else {
          sb.append(body + "\n");
          sb.append("\n");
        }
        writer.write(sb.toString());
        writer.flush();
        writer.close();
        logger.debug("write " + sb.length() + " characters.");
      } else {
        status = Status.BACKOFF;
      }
      transaction.commit();
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to write event.", ex);
    } finally {
      transaction.close();
    }
    return status;
  }
}
