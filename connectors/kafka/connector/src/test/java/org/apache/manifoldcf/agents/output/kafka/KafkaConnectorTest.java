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

import java.io.File;
import java.io.IOException;
import java.util.Date;
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
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KafkaConnectorTest {

  @Mock
  private KafkaProducer producer;

  private KafkaOutputConnector connector;

  @Before
  public void setup() throws Exception {
    connector = new KafkaOutputConnector();
    connector.setProducer(producer);
  }

  @Test
  public void whenSendingDocumenttoKafka() throws Exception {
    RepositoryDocument document = new RepositoryDocument();
    KafkaMessage kafkaMessage = new KafkaMessage();
    byte[] finalString = kafkaMessage.createJSON(document);

    document.addField("allow_token_document", "__nosecurity__");
    document.addField("deny_token_document", "__nosecurity__");
    document.addField("allow_token_share", "__nosecurity__");
    document.addField("deny_token_share", "__nosecurity__");
    document.addField("allow_token_parent", "__nosecurity__");
    document.addField("deny_token_parent", "__nosecurity__");
    document.addField("_name", "test.txt");
    document.addField("_content", "testDocument");

    /*VersionContext v = new VersionContext(null, null, null);
    IOutputAddActivity a = new IOutputAddActivity() {

      @Override
      public int sendDocument(String documentURI, RepositoryDocument document) throws ManifoldCFException, ServiceInterruption, IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public void noDocument() throws ManifoldCFException, ServiceInterruption {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public String qualifyAccessToken(String authorityNameString, String accessToken) throws ManifoldCFException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public void recordActivity(Long startTime, String activityType, Long dataSize, String entityURI, String resultCode, String resultDescription) throws ManifoldCFException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean checkDateIndexable(Date date) throws ManifoldCFException, ServiceInterruption {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean checkMimeTypeIndexable(String mimeType) throws ManifoldCFException, ServiceInterruption {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean checkDocumentIndexable(File localFile) throws ManifoldCFException, ServiceInterruption {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean checkLengthIndexable(long length) throws ManifoldCFException, ServiceInterruption {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }

      @Override
      public boolean checkURLIndexable(String url) throws ManifoldCFException, ServiceInterruption {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
      }
    };*/
    
    
    //connector.addOrReplaceDocumentWithException("document_uri", v, document, "",a);
    
    //connector.addOrReplaceDocumentWithException("document_uri", Mockito.any(VersionContext.class), document, "", Mockito.any(IOutputAddActivity.class));
    
    ProducerRecord record = new ProducerRecord("topic", finalString);
    verify(producer).send(record);
    
    connector.addOrReplaceDocumentWithException("document_uri", Mockito.any(VersionContext.class), document, "", Mockito.any(IOutputAddActivity.class));
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
