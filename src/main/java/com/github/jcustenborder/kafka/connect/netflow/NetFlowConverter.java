/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.netflow;

import com.github.jcustenborder.netty.netflow.v9.NetFlowV9Decoder;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

class NetFlowConverter {
  static final Map<String, ?> SOURCE_PARTITION = ImmutableMap.of();
  static final Map<String, ?> SOURCE_OFFSET = ImmutableMap.of();
  static final Schema KEY_SCHEMA;
  static final Schema VALUE_SCHEMA;
  static final Schema FLOWSET_SCHEMA;
  static final Schema TEMPLATE_FIELD_SCHEMA;
  private static final String FIELD_SENDER = "sender";
  private static final String FIELD_RECIPIENT = "recipient";
  private static final String FIELD_VERSION = "version";
  private static final String FIELD_COUNT = "count";
  private static final String FIELD_SOURCEID = "sourceID";
  private static final String FIELD_TIMESTAMP = "timestamp";
  private static final String FIELD_FLOWSETID = "flowsetID";
  private static final String FIELD_DATA = "data";
  private static final String FIELD_TYPE = "type";
  private static final String FIELD_LENGTH = "length";
  private static final String FIELD_FIELDS = "fields";
  private static final String FIELD_FLOWSETS = "flowsets";
  private static final String FIELD_TEMPLATEID = "templateID";

  static {
    KEY_SCHEMA = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.netflow.NetFlowKey")
        .field(FIELD_SENDER, SchemaBuilder.string().doc("The address of the host sending the NetFlow data.").build())
        .build();

    TEMPLATE_FIELD_SCHEMA = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.netflow.TemplateField")
        .field(FIELD_TYPE, SchemaBuilder.int16().doc("The version of NetFlow data received.").build())
        .field(FIELD_LENGTH, SchemaBuilder.int16().doc("The version of NetFlow data received.").build())
        .build();

    FLOWSET_SCHEMA = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.netflow.FlowSet")
        .field(FIELD_FLOWSETID, SchemaBuilder.int16().doc("The id for the flowset.").build())
        .field(FIELD_TYPE, SchemaBuilder.string().doc("The type of ").build())
        .field(FIELD_DATA, SchemaBuilder.bytes().optional().doc("The data for the flowset.").build())
        .field(FIELD_FIELDS, SchemaBuilder.array(TEMPLATE_FIELD_SCHEMA).doc("The data for the flowset.").build())
        .build();

    VALUE_SCHEMA = SchemaBuilder.struct()
        .name("com.github.jcustenborder.kafka.connect.netflow.NetFlowMessage")
        .field(FIELD_SENDER, SchemaBuilder.string().doc("The address of the host sending the NetFlow data.").build())
        .field(FIELD_RECIPIENT, SchemaBuilder.string().doc("The address of the host receiving the NetFlow data.").build())
        .field(FIELD_VERSION, SchemaBuilder.int16().doc("The version of NetFlow data received.").build())
        .field(FIELD_COUNT, SchemaBuilder.int16().doc("The count of data flows in the message.").build())
        .field(FIELD_RECIPIENT, SchemaBuilder.string().doc("The address of the host receiving the NetFlow data.").build())
        .field(FIELD_SOURCEID, SchemaBuilder.string().doc("The sourceID of the message.").build())
        .field(FIELD_TIMESTAMP, Timestamp.builder().doc("Timestamp of the message. Resolution is in seconds.").build())
        .field(FIELD_FLOWSETS, SchemaBuilder.array(FLOWSET_SCHEMA).doc("The flowsets that were exported.").build())
        .build();
  }

  final NetFlowSourceConnectorConfig config;

  NetFlowConverter(NetFlowSourceConnectorConfig config) {
    this.config = config;
  }

  SourceRecord convertFlow(NetFlowV9Decoder.NetFlowMessage message) {
    Struct keyStruct = new Struct(KEY_SCHEMA);
    keyStruct.put(FIELD_SENDER, message.sender().toString());

    long timestamp = (long) message.timestamp() * 1000L;
    Struct valueStruct = new Struct(VALUE_SCHEMA)
        .put(FIELD_SENDER, message.sender().toString())
        .put(FIELD_RECIPIENT, message.recipient().toString())
        .put(FIELD_VERSION, message.version())
        .put(FIELD_COUNT, message.count())
        .put(FIELD_SOURCEID, message.sourceID())
        .put(FIELD_TIMESTAMP, new Date(timestamp));

    for (NetFlowV9Decoder.FlowSet flowSet : message.flowsets()) {
      Struct flowSetStruct = new Struct(FLOWSET_SCHEMA);
      flowSetStruct.put(FIELD_FLOWSETID, flowSet.flowsetID());

      if (flowSet instanceof NetFlowV9Decoder.DataFlowSet) {
        NetFlowV9Decoder.DataFlowSet dataFlowSet = (NetFlowV9Decoder.DataFlowSet) flowSet;
        flowSetStruct.put(FIELD_DATA, dataFlowSet.data());
      } else if (flowSet instanceof NetFlowV9Decoder.TemplateFlowSet) {
        List<Struct> templateFields = new ArrayList<>();
        NetFlowV9Decoder.TemplateFlowSet templateFlowSet = (NetFlowV9Decoder.TemplateFlowSet) flowSet;
        flowSetStruct.put(FIELD_TEMPLATEID, templateFlowSet.templateID());

        for (NetFlowV9Decoder.TemplateField templateField : templateFlowSet.fields()) {
          Struct templateFieldStruct = new Struct(TEMPLATE_FIELD_SCHEMA)
              .put(FIELD_TYPE, templateField.type())
              .put(FIELD_LENGTH, templateField.length());
          templateFields.add(templateFieldStruct);
        }
        flowSetStruct.put(FIELD_FLOWSETS, templateFields);
      }
    }

    return new SourceRecord(
        SOURCE_PARTITION,
        SOURCE_OFFSET,
        this.config.topic,
        null,
        KEY_SCHEMA,
        keyStruct,
        VALUE_SCHEMA,
        valueStruct,
        timestamp
    );
  }
}
