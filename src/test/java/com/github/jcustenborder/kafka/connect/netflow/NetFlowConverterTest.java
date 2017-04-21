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
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NetFlowConverterTest {

  NetFlowConverter converter;

  @BeforeEach
  public void setup() {
    Map<String, String> settings = ImmutableMap.of(
        NetFlowSourceConnectorConfig.TOPIC_DATA_CONF, "netflow.data"
    );
    NetFlowSourceConnectorConfig config = new NetFlowSourceConnectorConfig(settings);
    this.converter = new NetFlowConverter(config);
  }

  @Test
  public void foo() {
    NetFlowV9Decoder.NetFlowMessage message = mock(NetFlowV9Decoder.NetFlowMessage.class);

//    SourceRecord sourceRecord = this.converter.convertFlow(message);
//    assertNotNull(sourceRecord);
  }

}
