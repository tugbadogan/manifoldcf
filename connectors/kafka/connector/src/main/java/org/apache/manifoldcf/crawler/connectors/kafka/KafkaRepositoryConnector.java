/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.manifoldcf.crawler.connectors.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import java.io.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

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
import org.apache.manifoldcf.crawler.system.Logging;

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
  protected final static String ACTIVITY_READ = "read document";
  public final static String ACTIVITY_FETCH = "fetch";

  protected static final String[] activitiesList = new String[]{ACTIVITY_READ, ACTIVITY_FETCH};
  KafkaConsumer consumer = null;

  @Override
  public int getConnectorModel() {
    return MODEL_ADD_CHANGE_DELETE; // We return only incremental documents.
  }

  public void setConsumer(KafkaConsumer consumer) {
    this.consumer = consumer;
  }

  @Override
  public void connect(ConfigParams config) {
    super.connect(config);

    String IP = params.getParameter(KafkaConfig.IP);
    String PORT = params.getParameter(KafkaConfig.PORT);

    Properties props = new Properties();
    props.put("metadata.broker.list", IP + ":" + PORT);
    props.put("bootstrap.servers", IP + ":" + PORT);
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("partition.assignment.strategy", "range");
    consumer = new KafkaConsumer<String, String>(props);
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
    try {
      //List<PartitionInfo> partitions = producer.partitionsFor(params.getParameter(KafkaConfig.TOPIC));
      return super.check();
    } catch (Exception e) {
      return "Transient error: " + e.getMessage();
    }

    /*try {
     // We really want to do something more like fetching a document here...
     alfrescoClient.fetchUserAuthorities("admin");
     return super.check();
     } catch (AlfrescoDownException e) {
     if (Logging.connectors != null) {
     Logging.connectors.warn(e.getMessage(), e);
     }
     return "Connection failed: " + e.getMessage();
     }*/
  }

  @Override
  public void disconnect() throws ManifoldCFException {
    consumer = null;
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
    /*try {
     long lastTransactionId = 0;
     long lastAclChangesetId = 0;

     if (lastSeedVersion != null && !lastSeedVersion.isEmpty()) {
     StringTokenizer tokenizer = new StringTokenizer(lastSeedVersion, "|");

     if (tokenizer.countTokens() == 2) {
     lastTransactionId = new Long(tokenizer.nextToken());
     lastAclChangesetId = new Long(tokenizer.nextToken());
     }
     }

     if (Logging.connectors != null && Logging.connectors.isDebugEnabled()) {
     Logging.connectors.debug(MessageFormat.format("Starting from transaction id: {0} and acl changeset id: {1}", new Object[]{lastTransactionId, lastAclChangesetId}));
     }

     long transactionIdsProcessed;
     long aclChangesetsProcessed;
     do {
     final AlfrescoResponse response = alfrescoClient.
     fetchNodes(lastTransactionId,
     lastAclChangesetId,
     ConfigurationHandler.getFilters(spec));
     int count = 0;
     for (Map<String, Object> doc : response.getDocuments()) {
     //          String json = gson.toJson(doc);
     //          activities.addSeedDocument(json);
     String uuid = doc.get("uuid").toString();
     activities.addSeedDocument(uuid);
     count++;
     }
     if (Logging.connectors != null && Logging.connectors.isDebugEnabled()) {
     Logging.connectors.debug(MessageFormat.format("Fetched and added {0} seed documents", new Object[]{new Integer(count)}));
     }

     transactionIdsProcessed = response.getLastTransactionId() - lastTransactionId;
     aclChangesetsProcessed = response.getLastAclChangesetId() - lastAclChangesetId;

     lastTransactionId = response.getLastTransactionId();
     lastAclChangesetId = response.getLastAclChangesetId();

     if (Logging.connectors != null && Logging.connectors.isDebugEnabled()) {
     Logging.connectors.debug(MessageFormat.format("transaction_id={0}, acl_changeset_id={1}", new Object[]{lastTransactionId, lastAclChangesetId}));
     }
     } while (transactionIdsProcessed > 0 || aclChangesetsProcessed > 0);

     if (Logging.connectors != null && Logging.connectors.isDebugEnabled()) {
     Logging.connectors.debug(MessageFormat.format("Recording {0} as last transaction id and {1} as last changeset id", new Object[]{lastTransactionId, lastAclChangesetId}));
     }
     return lastTransactionId + "|" + lastAclChangesetId;
     } catch (AlfrescoDownException e) {
     handleAlfrescoDownException(e, "seeding");
     return null;
     }*/
    return "";
  }

  @Override
  public void processDocuments(String[] documentIdentifiers, IExistingVersions statuses, Specification spec,
          IProcessActivity activities, int jobMode, boolean usesDefaultAuthority)
          throws ManifoldCFException, ServiceInterruption {

    /*boolean enableDocumentProcessing = ConfigurationHandler.getEnableDocumentProcessing(spec);
     for (String doc : documentIdentifiers) {

     String errorCode = null;
     String errorDesc = null;
     Long fileLengthLong = null;
     long startTime = System.currentTimeMillis();

     try {

     String nextVersion = statuses.getIndexedVersionString(doc);

     // Calling again Alfresco API because Document's actions are lost from seeding method
     AlfrescoResponse response = alfrescoClient.fetchNode(doc);
     if (response.getDocumentList().isEmpty()) { // Not found seeded document. Could reflect an error in Alfresco
     if (Logging.connectors != null) {
     Logging.connectors.warn(MessageFormat.format("Invalid Seeded Document from Alfresco with ID {0}", new Object[]{doc}));
     }
     activities.deleteDocument(doc);
     continue;
     }
     Map<String, Object> map = response.getDocumentList().get(0); // Should be only one
     if ((Boolean) map.get("deleted")) {
     activities.deleteDocument(doc);
     continue;
     }

     // From the map, get the things we know about
     String uuid = doc;
     String nodeRef = map.containsKey(FIELD_NODEREF) ? map.get(FIELD_NODEREF).toString() : "";
     String type = map.containsKey(FIELD_TYPE) ? map.get(FIELD_TYPE).toString() : "";
     String name = map.containsKey(FIELD_NAME) ? map.get(FIELD_NAME).toString() : "";

     // Fetch document metadata
     Map<String, Object> properties = alfrescoClient.fetchMetadata(uuid);

     // Process various special fields
     Object mdObject;

     // Size
     Long lSize = null;
     mdObject = properties.get(SIZE_PROPERTY);
     if (mdObject != null) {
     String size = mdObject.toString();
     lSize = new Long(size);
     }

     // Modified Date
     Date modifiedDate = null;
     mdObject = properties.get(MODIFIED_DATE_PROPERTY);
     if (mdObject != null) {
     modifiedDate = DateParser.parseISO8601Date(mdObject.toString());
     }

     // Created Date
     Date createdDate = null;
     mdObject = properties.get(CREATED_DATE_PROPERTY);
     if (mdObject != null) {
     createdDate = DateParser.parseISO8601Date(mdObject.toString());
     }

     // Establish the document version.
     if (modifiedDate == null) {
     activities.deleteDocument(doc);
     continue;
     }

     String documentVersion = (enableDocumentProcessing ? "+" : "-") + new Long(modifiedDate.getTime()).toString();

     if (!activities.checkDocumentNeedsReindexing(doc, documentVersion)) {
     continue;
     }

     String mimeType = null;
     Object mimetypeObject = properties.get(MIMETYPE_PROPERTY);
     if (mimetypeObject != null) {
     mimeType = mimetypeObject.toString();
     }

     if (lSize != null && !activities.checkLengthIndexable(lSize.longValue())) {
     activities.noDocument(doc, documentVersion);
     errorCode = activities.EXCLUDED_LENGTH;
     errorDesc = "Excluding document because of length (" + lSize + ")";
     continue;
     }

     if (!activities.checkMimeTypeIndexable(mimeType)) {
     activities.noDocument(doc, documentVersion);
     errorCode = activities.EXCLUDED_MIMETYPE;
     errorDesc = "Excluding document because of mime type (" + mimeType + ")";
     continue;
     }

     if (!activities.checkDateIndexable(modifiedDate)) {
     activities.noDocument(doc, documentVersion);
     errorCode = activities.EXCLUDED_DATE;
     errorDesc = "Excluding document because of date (" + modifiedDate + ")";
     continue;
     }

     String contentUrlPath = (String) properties.get(CONTENT_URL_PROPERTY);
     if (contentUrlPath == null || contentUrlPath.isEmpty()) {
     activities.noDocument(doc, documentVersion);
     errorCode = "NOURL";
     errorDesc = "Excluding document because no URL found";
     continue;
     }

     if (!activities.checkURLIndexable(contentUrlPath)) {
     activities.noDocument(doc, documentVersion);
     errorCode = activities.EXCLUDED_URL;
     errorDesc = "Excluding document because of URL ('" + contentUrlPath + "')";
     continue;
     }

     RepositoryDocument rd = new RepositoryDocument();
     rd.addField(FIELD_NODEREF, nodeRef);
     rd.addField(FIELD_TYPE, type);
     rd.setFileName(name);

     if (modifiedDate != null) {
     rd.setModifiedDate(modifiedDate);
     }

     if (createdDate != null) {
     rd.setCreatedDate(createdDate);
     }

     for (String property : properties.keySet()) {
     Object propertyValue = properties.get(property);
     rd.addField(property, propertyValue.toString());
     }

     if (mimeType != null && !mimeType.isEmpty()) {
     rd.setMimeType(mimeType);
     }

     // Indexing Permissions
     @SuppressWarnings("unchecked")
     List<String> permissions = (List<String>) properties.remove(AUTHORITIES_PROPERTY);
     if (permissions != null) {
     rd.setSecurityACL(RepositoryDocument.SECURITY_TYPE_DOCUMENT,
     permissions.toArray(new String[permissions.size()]));
     }

     // Document Binary Content
     InputStream stream;
     long length;
     byte[] empty = new byte[0];

     if (enableDocumentProcessing) {
     if (lSize != null) {
     stream = alfrescoClient.fetchContent(contentUrlPath);
     if (stream == null) {
     activities.noDocument(doc, documentVersion);
     errorCode = "NOSTREAM";
     errorDesc = "Excluding document because no content stream found";
     continue;
     }
     length = lSize.longValue();
     } else {
     stream = new ByteArrayInputStream(empty);
     length = 0L;
     }
     } else {
     stream = new ByteArrayInputStream(empty);
     length = 0L;
     }

     try {
     rd.setBinary(stream, length);
     if (Logging.connectors != null && Logging.connectors.isDebugEnabled()) {
     Logging.connectors.debug(MessageFormat.format("Ingesting with id: {0}, URI {1} and rd {2}", new Object[]{uuid, nodeRef, rd.getFileName()}));
     }
     activities.ingestDocumentWithException(doc, documentVersion, contentUrlPath, rd);
     errorCode = "OK";
     fileLengthLong = new Long(length);
     } catch (IOException e) {
     handleIOException(e, "reading stream");
     } finally {
     try {
     stream.close();
     } catch (IOException e) {
     handleIOException(e, "closing stream");
     }
     }

     } catch (AlfrescoDownException e) {
     handleAlfrescoDownException(e, "processing");
     } catch (ManifoldCFException e) {
     if (e.getErrorCode() == ManifoldCFException.INTERRUPTED) {
     errorCode = null;
     }
     throw e;
     } finally {
     if (errorCode != null) {
     activities.recordActivity(new Long(startTime), ACTIVITY_FETCH,
     fileLengthLong, doc, errorCode, errorDesc, null);
     }
     }
     }*/
  }

  protected final static long interruptionRetryTime = 5L * 60L * 1000L;

  /*
   protected static void handleAlfrescoDownException(AlfrescoDownException e, String context)
   throws ManifoldCFException, ServiceInterruption {
   long currentTime = System.currentTimeMillis();

   // Server doesn't appear to by up.  Try for a brief time then give up.
   String message = "Server appears down during " + context + ": " + e.getMessage();
   Logging.connectors.warn(message, e);
   throw new ServiceInterruption(message,
   e,
   currentTime + interruptionRetryTime,
   -1L,
   3,
   true);
   }
   */
  protected static void handleIOException(IOException e, String context)
          throws ManifoldCFException, ServiceInterruption {
    if ((e instanceof InterruptedIOException) && (!(e instanceof java.net.SocketTimeoutException))) {
      throw new ManifoldCFException(e.getMessage(), ManifoldCFException.INTERRUPTED);
    }

    long currentTime = System.currentTimeMillis();

    if (e instanceof java.net.ConnectException) {
      // Server isn't up at all.  Try for a brief time then give up.
      String message = "Server could not be contacted during " + context + ": " + e.getMessage();
      Logging.connectors.warn(message, e);
      throw new ServiceInterruption(message,
              e,
              currentTime + interruptionRetryTime,
              -1L,
              3,
              true);
    }

    if (e instanceof java.net.SocketTimeoutException) {
      String message2 = "Socket timeout exception during " + context + ": " + e.getMessage();
      Logging.connectors.warn(message2, e);
      throw new ServiceInterruption(message2,
              e,
              currentTime + interruptionRetryTime,
              currentTime + 20L * 60000L,
              -1,
              false);
    }

    if (e.getClass().getName().equals("java.net.SocketException")) {
      // In the past we would have treated this as a straight document rejection, and
      // treated it in the same manner as a 400.  The reasoning is that the server can
      // perfectly legally send out a 400 and drop the connection immediately thereafter,
      // this a race condition.
      // However, Solr 4.0 (or the Jetty version that the example runs on) seems
      // to have a bug where it drops the connection when two simultaneous documents come in
      // at the same time.  This is the final version of Solr 4.0 so we need to deal with
      // this.
      if (e.getMessage().toLowerCase(Locale.ROOT).indexOf("broken pipe") != -1
              || e.getMessage().toLowerCase(Locale.ROOT).indexOf("connection reset") != -1
              || e.getMessage().toLowerCase(Locale.ROOT).indexOf("target server failed to respond") != -1) {
        // Treat it as a service interruption, but with a limited number of retries.
        // In that way we won't burden the user with a huge retry interval; it should
        // give up fairly quickly, and yet NOT give up if the error was merely transient
        String message = "Server dropped connection during " + context + ": " + e.getMessage();
        Logging.connectors.warn(message, e);
        throw new ServiceInterruption(message,
                e,
                currentTime + interruptionRetryTime,
                -1L,
                3,
                false);
      }

      // Other socket exceptions are service interruptions - but if we keep getting them, it means 
      // that a socket timeout is probably set too low to accept this particular document.  So
      // we retry for a while, then skip the document.
      String message2 = "Socket exception during " + context + ": " + e.getMessage();
      Logging.connectors.warn(message2, e);
      throw new ServiceInterruption(message2,
              e,
              currentTime + interruptionRetryTime,
              currentTime + 20L * 60000L,
              -1,
              false);
    }

    // Otherwise, no idea what the trouble is, so presume that retries might fix it.
    String message3 = "IO exception during " + context + ": " + e.getMessage();
    Logging.connectors.warn(message3, e);
    throw new ServiceInterruption(message3,
            e,
            currentTime + interruptionRetryTime,
            currentTime + 2L * 60L * 60000L,
            -1,
            true);
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
