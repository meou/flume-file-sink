/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.meou.flume.sink.file;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestTextFileSink {

  private TextFileSink sink;

  @Test
  public void testNormal()
    throws EventDeliveryException, FileNotFoundException, IOException {
    sink = new TextFileSink();

    Context context = new Context();
    context.put("filename", "target/testNormal.log");

    Channel channel = new PseudoTxnMemoryChannel();

    Configurables.configure(channel, context);
    Configurables.configure(sink, context);
    sink.setChannel(channel);

    List<String> answer = new ArrayList<>();
    sink.start();
    for (int i = 1; i <= 10; i++) {
      SimpleEvent event = new SimpleEvent();
      Map<String, String> eventHeaders = new HashMap<>();

      String key = "key-" + Integer.toString(i);
      String value = "value-" + Integer.toString(i);
      answer.add(key + ":" + value);
      eventHeaders.put(key,value);
      event.setHeaders(eventHeaders);

      StringBuffer sb = new StringBuffer();
      for (int j = 1 ; j <= i*100; j++) {
        sb.append(Integer.toString(j));
      }
      String eventBody = sb.toString();
      answer.add(eventBody);
      event.setBody(eventBody.getBytes());
      channel.put(event);
      sink.process();
    }
    sink.stop();

    BufferedReader br = new BufferedReader(
      new FileReader("target/testNormal.log"));
    String line = null;
    int i = 0;
    while ((line = br.readLine()) != null) {
      if (line.length() == 0) {
        continue;
      }
      assertTrue(line.equals(answer.get(i)));
      i++;
    }
  }

  @Test
  public void testDelimiter()
    throws EventDeliveryException, FileNotFoundException, IOException {
    sink = new TextFileSink();

    Context context = new Context();
    context.put("filename", "target/testDelimiter.log");
    context.put("delimiter", ",");

    Channel channel = new PseudoTxnMemoryChannel();

    Configurables.configure(channel, context);
    Configurables.configure(sink, context);
    sink.setChannel(channel);

    sink.start();
    SimpleEvent event = new SimpleEvent();
    Map<String, String> eventHeaders = new HashMap<>();
    eventHeaders.put("k","v");
    event.setHeaders(eventHeaders);
    event.setBody("".getBytes());
    channel.put(event);
    sink.process();
    sink.stop();

    BufferedReader br = new BufferedReader(
      new FileReader("target/testDelimiter.log"));
    String line = br.readLine();
    assertTrue(line.equals("k,v"));
  }

  @Test
  public void testNoHeader()
    throws EventDeliveryException, FileNotFoundException, IOException {
    sink = new TextFileSink();

    Context context = new Context();
    context.put("filename", "target/testNoHeader.log");
    context.put("headerIncluded", "false");

    Channel channel = new PseudoTxnMemoryChannel();

    Configurables.configure(channel, context);
    Configurables.configure(sink, context);
    sink.setChannel(channel);

    sink.start();
    SimpleEvent event = new SimpleEvent();
    Map<String, String> eventHeaders = new HashMap<>();
    eventHeaders.put("k","v");
    event.setHeaders(eventHeaders);
    event.setBody("".getBytes());
    channel.put(event);
    sink.process();
    sink.stop();

    BufferedReader br = new BufferedReader(
      new FileReader("target/testNoHeader.log"));
    String line = null;
    while ((line = br.readLine()) != null) {
      assertTrue(line.length() == 0);
    }
  }

  @Test(expected=EventDeliveryException.class)
  public void testInvalidFilename() throws EventDeliveryException {
    sink = new TextFileSink();

    Context context = new Context();
    context.put("filename", "/NoOK.log");
    Channel channel = new PseudoTxnMemoryChannel();
    Configurables.configure(channel, context);
    Configurables.configure(sink, context);
    sink.setChannel(channel);

    sink.start();
    channel.put(new SimpleEvent());
    sink.process();
    sink.stop();
  }
}
