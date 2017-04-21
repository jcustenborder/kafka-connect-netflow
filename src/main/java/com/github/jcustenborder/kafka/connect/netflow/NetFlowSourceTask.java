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

import com.github.jcustenborder.kafka.connect.utils.VersionUtil;
import com.github.jcustenborder.kafka.connect.utils.data.SourceRecordConcurrentLinkedDeque;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NetFlowSourceTask extends SourceTask {
  private static final Logger log = LoggerFactory.getLogger(NetFlowSourceTask.class);
  final Time time = new SystemTime();
  NetFlowSourceConnectorConfig config;
  ServiceManager serviceManager;
  SourceRecordConcurrentLinkedDeque records;

  @Override
  public String version() {
    return VersionUtil.version(this.getClass());
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new NetFlowSourceConnectorConfig(map);
    this.records = new SourceRecordConcurrentLinkedDeque();
    this.serviceManager = new ServiceManager(
        Arrays.asList(new NetFlowListeningService(this.config, records, time))
    );
    log.info("Starting StatsDListeningService");
    this.serviceManager.startAsync();
    try {
      this.serviceManager.awaitHealthy(60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException(e);
    }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    List<SourceRecord> recordBatch = new ArrayList<>(512);

    while (!this.records.drain(recordBatch)) {
      Thread.sleep(250);
    }

    return recordBatch;
  }

  @Override
  public void stop() {
    this.serviceManager.stopAsync();
    try {
      this.serviceManager.awaitStopped(60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new ConnectException(e);
    }
  }
}