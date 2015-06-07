/* $Id: KafkaConfig.java 1299512 2012-03-12 00:58:38Z piergiorgio $ */

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

package org.apache.manifoldcf.agents.output.kafka;

import org.apache.manifoldcf.core.interfaces.ConfigParams;
import org.apache.manifoldcf.core.interfaces.IPostParameters;

public class KafkaConfig extends KafkaParam
{

  /**
	 * 
	 */
  private static final long serialVersionUID = -2071296573398352538L;

  /** Parameters used for the configuration */
  final private static ParameterEnum[] CONFIGURATIONLIST =
  { ParameterEnum.IP, ParameterEnum.Port,
      ParameterEnum.Topic};

  /** Build a set of KafkaParameters by reading ConfigParams. If the
   * value returned by ConfigParams.getParameter is null, the default value is
   * set.
   * 
   * @param paramList
   * @param params */
  public KafkaConfig(ConfigParams params)
  {
    super(CONFIGURATIONLIST);
    for (ParameterEnum param : CONFIGURATIONLIST)
    {
      String value = params.getParameter(param.name());
      if (value == null)
        value = param.defaultValue;
      put(param, value);
    }
  }

  /** @return a unique identifier for one index on one Kafka instance. */
  public String getUniqueIndexIdentifier()
  {
    StringBuffer sb = new StringBuffer();
    sb.append(getIP());
    if (sb.charAt(sb.length() - 1) != '/')
      sb.append('/');
    sb.append(getPort());
    return sb.toString();
  }

  public final static void contextToConfig(IPostParameters variableContext,
      ConfigParams parameters)
  {
    for (ParameterEnum param : CONFIGURATIONLIST)
    {
      String p = variableContext.getParameter(param.name().toLowerCase());
      if (p != null)
        parameters.setParameter(param.name(), p);
    }
  }

  final public String getIP()
  {
    return get(ParameterEnum.IP);
  }

  final public String getPort()
  {
    return get(ParameterEnum.Port);
  }

  final public String getTopic()
  {
    return get(ParameterEnum.Topic);
  }

}
