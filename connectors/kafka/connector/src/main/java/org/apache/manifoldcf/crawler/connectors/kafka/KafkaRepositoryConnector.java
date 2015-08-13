/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
* http://www.apache.org/licenses/LICENSE-2.0
 * 
* Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.manifoldcf.crawler.connectors.kafka;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

//import org.apache.manifoldcf.agents.output.kafka.KafkaConfig;
import static org.apache.manifoldcf.agents.output.kafka.KafkaConfig.IP;
import static org.apache.manifoldcf.agents.output.kafka.KafkaConfig.IP_DEFAULT;
import static org.apache.manifoldcf.agents.output.kafka.KafkaConfig.PORT;
import static org.apache.manifoldcf.agents.output.kafka.KafkaConfig.PORT_DEFAULT2;
import static org.apache.manifoldcf.agents.output.kafka.KafkaConfig.TOPIC;
import static org.apache.manifoldcf.agents.output.kafka.KafkaConfig.TOPIC_DEFAULT;
import org.apache.manifoldcf.agents.output.kafka.Messages;
import org.apache.manifoldcf.core.interfaces.ConfigParams;
import org.apache.manifoldcf.core.interfaces.IHTTPOutput;
import org.apache.manifoldcf.core.interfaces.IPostParameters;
import org.apache.manifoldcf.core.interfaces.IThreadContext;
import org.apache.manifoldcf.core.interfaces.ManifoldCFException;
import org.apache.manifoldcf.crawler.connectors.BaseRepositoryConnector;
import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.agents.interfaces.*;
import org.apache.manifoldcf.agents.output.kafka.KafkaConfig;
import org.apache.manifoldcf.crawler.interfaces.IExistingVersions;
import org.apache.manifoldcf.crawler.interfaces.IProcessActivity;
import org.apache.manifoldcf.crawler.interfaces.ISeedingActivity;

/**
 *
 * @author tugba
 */
public class KafkaRepositoryConnector extends BaseRepositoryConnector {

  /**
   * Output the configuration header section. This method is called in the head
   * section of the connector's configuration page. Its purpose is to add the
   * required tabs to the list, and to output any javascript methods that might
   * be needed by the configuration editing HTML.
   *
   * @param threadContext is the local thread context.
   * @param out is the output to which any HTML should be sent.
   * @param parameters are the configuration parameters, as they currently
   * exist, for this connection being configured.
   * @param tabsArray is an array of tab names. Add to this array any tab names
   * that are specific to the connector.
   */
  public final static String ACTIVITY_FETCH = "fetch";

  protected static final String[] activitiesList = new String[]{ACTIVITY_FETCH};
  ConsumerConnector consumer = null;
  private ExecutorService executor;
  Map<String, String> messages = new HashMap<String, String>();

  @Override
  public int getConnectorModel() {
    return MODEL_ADD; // We return only incremental documents.
  }

  public void setConsumer(ConsumerConnector consumer) {
    this.consumer = consumer;
  }

  @Override
  public void connect(ConfigParams config) {
    super.connect(config);

    String IP = params.getParameter(KafkaConfig.IP);
    String PORT = params.getParameter(KafkaConfig.PORT);

    System.out.println("Connecting to zookeeper: " + IP + ":" + PORT);

    try {
      consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
              createConsumerConfig(IP + ":" + PORT, "testss"));
    } catch (Exception e) {
      e.printStackTrace();
      consumer = null;
    }
  }

  private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
    Properties props = new Properties();
    props.put("zookeeper.connect", a_zookeeper);
    props.put("group.id", a_groupId);
    props.put("zookeeper.session.timeout.ms", "4000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    props.put("auto.commit.enable", "false");
    props.put("auto.offset.reset", "smallest");
    props.put("consumer.timeout.ms", "20000");

    return new ConsumerConfig(props);
  }

  private static String getConfig(ConfigParams config,
          String parameter,
          String defaultValue) {
    final String protocol = config.getParameter(parameter);
    if (protocol == null) {
      return defaultValue;
    }
    return protocol;
  }

  private static String getObfuscatedConfig(ConfigParams config,
          String parameter,
          String defaultValue) {
    final String protocol = config.getObfuscatedParameter(parameter);
    if (protocol == null) {
      return defaultValue;
    }
    return protocol;
  }

  @Override
  public String check() throws ManifoldCFException {
    if (consumer == null) {
      return "Transient error: Cannot connect to zookeeper";
    }
    return super.check();
  }

  @Override
  public void disconnect() throws ManifoldCFException {
    System.out.println("Disconnecting.....");
    consumer.shutdown();
    super.disconnect();
  }

  @Override
  public String[] getActivitiesList() {
    return activitiesList;
  }

  @Override
  public int getMaxDocumentRequest() {
    return 20;
  }

  @Override
  public String addSeedDocuments(ISeedingActivity activities, Specification spec,
          String lastSeedVersion, long seedTime, int jobMode) throws ManifoldCFException, ServiceInterruption {
    System.out.println("SEED FUNCTION");
    //activities.addSeedDocument("asdf");

    try {
      String topic = params.getParameter(KafkaConfig.TOPIC);
      Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
      topicCountMap.put(topic, new Integer(1));
      Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
      List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

      // now launch all the threads
      executor = Executors.newFixedThreadPool(1);
      System.out.println("CONTROL 1");

      int threadNumber = 0;
      /*if (streams == null) {
       System.out.println("STREAMS IS NULL");
       } else {
       System.out.println("STREAMS IS NOT NULL");
       }*/
      for (final KafkaStream stream : streams) {
        System.out.println("CONTROL 2");
        executor.execute(new ConsumerTest(stream, threadNumber, activities));
        System.out.println("CONTROL 3");
        threadNumber++;
      }

      executor.shutdown();
      System.out.println("CONTROL 4");
      executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      System.out.println("CONTROL 5");
      //disconnect();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return "";
  }

  @Override
  public void processDocuments(String[] documentIdentifiers, IExistingVersions statuses, Specification spec,
          IProcessActivity activities, int jobMode, boolean usesDefaultAuthority)
          throws ManifoldCFException, ServiceInterruption {

    System.out.println("Processing documents.....");
    RepositoryDocument rd = null;
    String message = null;
    String versionString;
    String uri;
    int length = 0;
    for (String documentIdentifier : documentIdentifiers) {
      rd = new RepositoryDocument();
      message = messages.get(documentIdentifier);
      System.out.println("MESSAGE IN PROCESS DOCUMENT : " + message);
      versionString = documentIdentifier;
      uri = documentIdentifier;
      length = message.length();
      byte[] content = message.getBytes(StandardCharsets.UTF_8);
      ByteArrayInputStream is = new ByteArrayInputStream(content);
      try {
        rd.setBinary(is, length);
        try {
          activities.ingestDocumentWithException(documentIdentifier, versionString, uri, rd);
          //messages.remove(documentIdentifier);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } finally {
        try {
          is.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public class ConsumerTest implements Runnable {

    private KafkaStream m_stream;
    private int m_threadNumber;
    private ISeedingActivity m_activities;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber, ISeedingActivity activities) {
      m_threadNumber = a_threadNumber;
      m_stream = a_stream;
      m_activities = activities;
    }

    @SuppressWarnings("empty-statement")
    public void run() {

      try {
        String hashcode = null;
        String message = null;

        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
          MessageAndMetadata<byte[], byte[]> it_next = it.next();
          hashcode = "" + (it_next.hashCode());
          System.out.println("HASH CODE: " + hashcode);
          message = new String(it_next.message());
          //System.out.println(message);
          //System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
          System.out.println("Thread " + m_threadNumber + ": " + message);
          messages.put(hashcode, message);
          //consumer.commitOffsets(true);
          System.out.println("ADDED TO HASH MAP");
          m_activities.addSeedDocument(hashcode);
          System.out.println("ADDSEEDDOCUMENT WAS DONE");
        }
        System.out.println("Shutting down Thread: " + m_threadNumber);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void outputConfigurationHeader(IThreadContext threadContext, IHTTPOutput out, Locale locale, ConfigParams parameters, List<String> tabsArray) throws ManifoldCFException, IOException {
    tabsArray.add(Messages.getString(locale, "KafkaConnector.Parameters"));

    out.print(
            "<script type=\"text/javascript\">\n"
            + "<!--\n"
            + "function checkConfigForSave()\n"
            + "{\n"
            + "  if (editconnection.ip.value == \"\")\n"
            + "  {\n"
            + "    alert(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.IPCannotBeNull") + "\");\n"
            + "    SelectTab(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.Parameters") + "\");\n"
            + "    editconnection.ip.focus();\n"
            + "    return false;\n"
            + "  }\n"
            + "  if (editconnection.port.value == \"\")\n"
            + "  {\n"
            + "    alert(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.PortCannotBeNull") + "\");\n"
            + "    SelectTab(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.Parameters") + "\");\n"
            + "    editconnection.port.focus();\n"
            + "    return false;\n"
            + "  }\n"
            + "  if (!isInteger(editconnection.port.value))\n"
            + "  {\n"
            + "    alert(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.PortMustBeAnInteger") + "\");\n"
            + "    SelectTab(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.Parameters") + "\");\n"
            + "    editconnection.port.focus();\n"
            + "    return false;\n"
            + "  }\n"
            + "  if (editconnection.topic.value == \"\")\n"
            + "  {\n"
            + "    alert(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.TopicCannotBeNull") + "\");\n"
            + "    SelectTab(\"" + Messages.getBodyJavascriptString(locale, "KafkaConnector.Parameters") + "\");\n"
            + "    editconnection.topic.focus();\n"
            + "    return false;\n"
            + "  }\n"
            + "  return true;\n"
            + "}\n"
            + "\n"
            + "//-->\n"
            + "</script>\n"
    );
  }

  /**
   * Output the configuration body section. This method is called in the body
   * section of the connector's configuration page. Its purpose is to present
   * the required form elements for editing. The coder can presume that the HTML
   * that is output from this configuration will be within appropriate <html>,
   * <body>, and <form> tags. The name of the form is "editconnection".
   *
   * @param threadContext is the local thread context.
   * @param out is the output to which any HTML should be sent.
   * @param parameters are the configuration parameters, as they currently
   * exist, for this connection being configured.
   * @param tabName is the current tab name.
   */
  public void outputConfigurationBody(IThreadContext threadContext, IHTTPOutput out, Locale locale, ConfigParams parameters, String tabName)
          throws ManifoldCFException, IOException {
    String ip = parameters.getParameter("ip");
    if (ip == null) {
      ip = "localhost";
    }

    String port = parameters.getParameter("port");
    if (port == null) {
      port = "2181";
    }

    String topic = parameters.getParameter("topic");
    if (topic == null) {
      topic = "topic";
    }

    if (tabName.equals(Messages.getString(locale, "KafkaConnector.Parameters"))) {
      out.print(
              "<table class=\"displaytable\">\n"
              + "  <tr>\n"
              + "    <td class=\"description\"><nobr>" + Messages.getBodyString(locale, "KafkaConnector.IPColon") + "</nobr></td>\n"
              + "    <td class=\"value\">\n"
              + "      <input name=\"ip\" type=\"text\" size=\"48\" value=\"" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(IP_DEFAULT) + "\"/>\n"
              + "    </td>\n"
              + "  </tr>\n"
              + "  <tr>\n"
              + "    <td class=\"description\"><nobr>" + Messages.getBodyString(locale, "KafkaConnector.PortColon") + "</nobr></td>\n"
              + "    <td class=\"value\">\n"
              + "      <input name=\"port\" type=\"text\" size=\"24\" value=\"" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(PORT_DEFAULT2) + "\"/>\n"
              + "    </td>\n"
              + "  </tr>\n"
              + "  <tr>\n"
              + "    <td class=\"description\"><nobr>" + Messages.getBodyString(locale, "KafkaConnector.TopicColon") + "</nobr></td>\n"
              + "    <td class=\"value\">\n"
              + "      <input name=\"topic\" type=\"text\" size=\"24\" value=\"" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(TOPIC_DEFAULT) + "\"/>\n"
              + "    </td>\n"
              + "  </tr>\n"
              + "</table>\n"
      );
    } else {
      // Server tab hiddens
      out.print(
              "<input type=\"hidden\" name=\"ip\" value=\"" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(IP) + "\"/>\n"
              + "<input type=\"hidden\" name=\"port\" value=\"" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(PORT) + "\"/>\n"
              + "<input type=\"hidden\" name=\"topic\" value=\"" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(TOPIC) + "\"/>\n"
      );
    }
  }

  /**
   * Process a configuration post. This method is called at the start of the
   * connector's configuration page, whenever there is a possibility that form
   * data for a connection has been posted. Its purpose is to gather form
   * information and modify the configuration parameters accordingly. The name
   * of the posted form is "editconnection".
   *
   * @param threadContext is the local thread context.
   * @param variableContext is the set of variables available from the post,
   * including binary file post information.
   * @param parameters are the configuration parameters, as they currently
   * exist, for this connection being configured.
   * @return null if all is well, or a string error message if there is an error
   * that should prevent saving of the connection (and cause a redirection to an
   * error page).
   */
  @Override
  public String processConfigurationPost(IThreadContext threadContext, IPostParameters variableContext, ConfigParams parameters)
          throws ManifoldCFException {
    String ip = variableContext.getParameter("ip");
    if (ip != null) {
      parameters.setParameter("ip", ip);
    }

    String port = variableContext.getParameter("port");
    if (port != null) {
      parameters.setParameter("port", port);
    }

    String topic = variableContext.getParameter("topic");
    if (topic != null) {
      parameters.setParameter("topic", topic);
    }

    return null;
  }

  /**
   * View configuration. This method is called in the body section of the
   * connector's view configuration page. Its purpose is to present the
   * connection information to the user. The coder can presume that the HTML
   * that is output from this configuration will be within appropriate <html>
   * and <body> tags.
   *
   * @param threadContext is the local thread context.
   * @param out is the output to which any HTML should be sent.
   * @param parameters are the configuration parameters, as they currently
   * exist, for this connection being configured.
   */
  @Override
  public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out, Locale locale, ConfigParams parameters)
          throws ManifoldCFException, IOException {
    String ip = parameters.getParameter("ip");
    String port = parameters.getParameter("port");
    String topic = parameters.getParameter("topic");

    out.print(
            "<table class=\"displaytable\">\n"
            + "  <tr>\n"
            + "    <td class=\"description\"><nobr>" + Messages.getBodyString(locale, "KafkaConnector.IPColon") + "</nobr></td>\n"
            + "    <td class=\"value\">\n" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(ip) + "</td>\n"
            + "  </tr>\n"
            + "  <tr>\n"
            + "    <td class=\"description\"><nobr>" + Messages.getBodyString(locale, "KafkaConnector.PortColon") + "</nobr></td>\n"
            + "    <td class=\"value\">\n" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(port) + "</td>\n"
            + "  </tr>\n"
            + "  <tr>\n"
            + "    <td class=\"description\"><nobr>" + Messages.getBodyString(locale, "KafkaConnector.TopicColon") + "</nobr></td>\n"
            + "    <td class=\"value\">\n" + org.apache.manifoldcf.ui.util.Encoder.attributeEscape(topic) + "</td>\n"
            + "  </tr>\n"
            + "</table>\n"
    );
  }
}
