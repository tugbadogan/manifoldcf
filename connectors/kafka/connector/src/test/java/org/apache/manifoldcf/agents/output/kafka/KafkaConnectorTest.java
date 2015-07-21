/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.manifoldcf.agents.output.kafka;

import static org.mockito.Mockito.verify;
import java.util.HashMap;
import org.apache.manifoldcf.agents.interfaces.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/*import com.github.maoo.indexer.client.AlfrescoClient;
 import com.github.maoo.indexer.client.AlfrescoFilters;
 import com.github.maoo.indexer.client.AlfrescoResponse;*/
import org.apache.manifoldcf.agents.interfaces.RepositoryDocument;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.core.interfaces.VersionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaConnectorTest {

  @Mock
  private KafkaProducer producer;

  private KafkaOutputConnector connector;
  RepositoryDocument document;

  @Before
  public void setup() throws Exception {
    connector = new KafkaOutputConnector();
    connector.setProducer(producer);
  }

  @Test
  public void whenSendingDocumenttoKafka() throws Exception {

    document = new RepositoryDocument();

    document.setMimeType("text\'/plain");
    document.setFileName("test.txt");

    KafkaMessage kafkaMessage = new KafkaMessage();
    byte[] finalString = kafkaMessage.createJSON(document);

    IOutputAddActivity activities = mock(IOutputAddActivity.class);
    VersionContext version = mock(VersionContext.class);
    ProducerRecord record = new ProducerRecord("topic", finalString);

    when(producer.send(Mockito.any(ProducerRecord.class))).thenReturn(null);

    connector.addOrReplaceDocumentWithException("document_uri", version, document, "", activities);
    verify(producer).send(record, null);
    //verify(producer).send(record).get();
  }

  @SuppressWarnings("serial")
  private class TestDocument extends HashMap<String, Object> {

    static final String IP = "localhost";
    static final String port = "9092";
    static final String topic = "topic";

    public TestDocument() {
      super();
      put("IP", IP);
      put("port", port);
      put("topic", topic);
    }

    public RepositoryDocument getRepositoryDocument() throws ManifoldCFException {
      RepositoryDocument rd = new RepositoryDocument();
      rd.setFileName("name");
      for (String property : keySet()) {
        rd.addField(property, get(property).toString());
      }
      return rd;
    }
  }
}
