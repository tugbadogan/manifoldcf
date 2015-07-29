/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
