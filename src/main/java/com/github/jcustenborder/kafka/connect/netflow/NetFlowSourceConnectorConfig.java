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

import com.github.jcustenborder.kafka.connect.utils.config.ValidPort;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

class NetFlowSourceConnectorConfig extends AbstractConfig {

  public static final String LISTEN_PORT_CONF = "listen.port";
  public static final String LISTEN_ADDRESS_CONF = "listen.address";
  public static final String TOPIC_DATA_CONF = "topic";
  static final String LISTEN_PORT_DOC = "";
  static final String LISTEN_ADDRESS_DOC = "";
  static final String TOPIC_DATA_DOC = "";

  public final int listenPort;
  public final String listenAddress;
  public final String topic;

  public NetFlowSourceConnectorConfig(Map<String, String> settings) {
    super(conf(), settings);
    this.listenPort = this.getInt(LISTEN_PORT_CONF);
    this.listenAddress = this.getString(LISTEN_ADDRESS_CONF);
    this.topic = this.getString(TOPIC_DATA_CONF);
  }


  public static ConfigDef conf() {
    return new ConfigDef()
        .define(LISTEN_PORT_CONF, ConfigDef.Type.INT, 12345, ValidPort.of(), ConfigDef.Importance.MEDIUM, LISTEN_PORT_DOC)
        .define(LISTEN_ADDRESS_CONF, ConfigDef.Type.STRING, "0.0.0.0", ConfigDef.Importance.MEDIUM, LISTEN_ADDRESS_DOC)
        .define(TOPIC_DATA_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TOPIC_DATA_DOC);
  }
}
