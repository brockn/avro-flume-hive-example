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
package com.cloudera.flume.serialization;

import java.io.OutputStream;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.AbstractAvroEventSerializer;

import com.google.common.base.Charsets;

public class FlumeEventStringBodyAvroEventSerializer extends AbstractAvroEventSerializer<FlumeEventStringBodyAvroEventSerializer.Container> {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{ \"type\":\"record\", \"name\": \"Event\", \"fields\": [" +
      " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
      " {\"name\": \"body\", \"type\": \"string\" } ] }");
  
  private final OutputStream out;

  private FlumeEventStringBodyAvroEventSerializer(OutputStream out) {
    this.out = out;
  }

  @Override
  protected Schema getSchema() {
    return SCHEMA;
  }

  @Override
  protected OutputStream getOutputStream() {
    return out;
  }

  /**
   * A no-op for this simple, special-case implementation
   * @param event
   * @return
   */
  @Override
  protected Container convert(Event event) {
    return new Container(event.getHeaders(), new String(event.getBody(), Charsets.UTF_8));
  }
  
  public static class Container {
    private final Map<String, String> headers;
    private final String body;
    public Container(Map<String, String> headers, String body) {
      super();
      this.headers = headers;
      this.body = body;
    }
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      FlumeEventStringBodyAvroEventSerializer writer = new FlumeEventStringBodyAvroEventSerializer(out);
      writer.configure(context);
      return writer;
    }

  }

}
