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

import org.apache.manifoldcf.core.interfaces.*;
import org.apache.manifoldcf.agents.interfaces.*;

import java.util.*;
import java.io.*;

/** This is a kafka output connector.
*/
public class KafkaOutputConnector extends org.apache.manifoldcf.agents.output.BaseOutputConnector
{
  public static final String _rcsid = "@(#)$Id: KafkaConnector.java 988245 2010-08-23 18:39:35Z kwright $";

  // Activities we log

  /** Ingestion activity */
  public final static String INGEST_ACTIVITY = "document ingest";
  /** Document removal activity */
  public final static String REMOVE_ACTIVITY = "document deletion";
  /** Job notify activity */
  public final static String JOB_COMPLETE_ACTIVITY = "output notification";
  
  private final static String KAFKA_TAB_PARAMETERS = "KafkaConnector.Parameters";

  /** Forward to the javascript to check the configuration parameters */
  private static final String EDIT_CONFIG_HEADER_FORWARD = "editConfiguration.js";
  
   /** Forward to the HTML template to edit the configuration parameters */
  private static final String EDIT_CONFIG_FORWARD_PARAMETERS = "editConfiguration_Parameters.html";

  /** Forward to the HTML template to view the configuration parameters */
  private static final String VIEW_CONFIG_FORWARD = "viewConfiguration.html";

  /** Constructor.
  */
  public KafkaOutputConnector()
  {
  }

  /** Return the list of activities that this connector supports (i.e. writes into the log).
  *@return the list.
  */
  @Override
  public String[] getActivitiesList()
  {
    return new String[]{INGEST_ACTIVITY,REMOVE_ACTIVITY,JOB_COMPLETE_ACTIVITY};
  }

  /** Connect.
  *@param configParameters is the set of configuration parameters, which
  * in this case describe the target appliance, basic auth configuration, etc.  (This formerly came
  * out of the ini file.)
  */
  @Override
  public void connect(ConfigParams configParameters)
  {
    super.connect(configParameters);
  }

  /** Close the connection.  Call this before discarding the connection.
  */
  @Override
  public void disconnect()
    throws ManifoldCFException
  {
    super.disconnect();
  }

  /** Set up a session */
  protected void getSession()
    throws ManifoldCFException, ServiceInterruption
  {
  }

  private static void outputResource(String resName, IHTTPOutput out,
      Locale locale, KafkaParam params,
      String tabName, Integer sequenceNumber, Integer currentSequenceNumber) throws ManifoldCFException
  {
    Map<String,String> paramMap = null;
    if (params != null) {
      paramMap = params.buildMap();
      if (tabName != null) {
        paramMap.put("TabName", tabName);
      }
      if (currentSequenceNumber != null)
        paramMap.put("SelectedNum",currentSequenceNumber.toString());
    }
    else
    {
      paramMap = new HashMap<String,String>();
    }
    if (sequenceNumber != null)
      paramMap.put("SeqNum",sequenceNumber.toString());

    Messages.outputResourceWithVelocity(out, locale, resName, paramMap, true);
  }
  
  @Override
  public void outputConfigurationHeader(IThreadContext threadContext,
      IHTTPOutput out, Locale locale, ConfigParams parameters,
      List<String> tabsArray) throws ManifoldCFException, IOException
  {
    super.outputConfigurationHeader(threadContext, out, locale, parameters,
        tabsArray);
    tabsArray.add(Messages.getString(locale, KAFKA_TAB_PARAMETERS));
    outputResource(EDIT_CONFIG_HEADER_FORWARD, out, locale, null, null, null, null);
  }
  
  @Override
  public void outputConfigurationBody(IThreadContext threadContext,
      IHTTPOutput out, Locale locale, ConfigParams parameters, String tabName)
      throws ManifoldCFException, IOException
  {
    super.outputConfigurationBody(threadContext, out, locale, parameters,
        tabName);
    KafkaConfig config = this.getConfigParameters(parameters);
    outputResource(EDIT_CONFIG_FORWARD_PARAMETERS, out, locale, config, tabName, null, null);
  }
  
  final private KafkaConfig getConfigParameters(
      ConfigParams configParams)
  {
    if (configParams == null)
      configParams = getConfiguration();
    return new KafkaConfig(configParams);
  }
  
  @Override
  public void viewConfiguration(IThreadContext threadContext, IHTTPOutput out,
      Locale locale, ConfigParams parameters) throws ManifoldCFException,
      IOException
  {
    outputResource(VIEW_CONFIG_FORWARD, out, locale,
        getConfigParameters(parameters), null, null, null);
  }
  
  @Override
  public String processConfigurationPost(IThreadContext threadContext,
      IPostParameters variableContext, ConfigParams parameters)
      throws ManifoldCFException
  {
    KafkaConfig.contextToConfig(variableContext, parameters);
    return null;
  }
  
  
  
  /** Test the connection.  Returns a string describing the connection integrity.
  *@return the connection's status as a displayable string.
  */
  @Override
  public String check()
    throws ManifoldCFException
  {
    try
    {
      getSession();
      return super.check();
    }
    catch (ServiceInterruption e)
    {
      return "Transient error: "+e.getMessage();
    }
  }

  /** Get an output version string, given an output specification.  The output version string is used to uniquely describe the pertinent details of
  * the output specification and the configuration, to allow the Connector Framework to determine whether a document will need to be output again.
  * Note that the contents of the document cannot be considered by this method, and that a different version string (defined in IRepositoryConnector)
  * is used to describe the version of the actual document.
  *
  * This method presumes that the connector object has been configured, and it is thus able to communicate with the output data store should that be
  * necessary.
  *@param spec is the current output specification for the job that is doing the crawling.
  *@return a string, of unlimited length, which uniquely describes output configuration and specification in such a way that if two such strings are equal,
  * the document will not need to be sent again to the output data store.
  */
  @Override
  public VersionContext getPipelineDescription(Specification spec)
    throws ManifoldCFException, ServiceInterruption
  {
    return new VersionContext("",params,spec);
  }

  /** Add (or replace) a document in the output data store using the connector.
  * This method presumes that the connector object has been configured, and it is thus able to communicate with the output data store should that be
  * necessary.
  * The OutputSpecification is *not* provided to this method, because the goal is consistency, and if output is done it must be consistent with the
  * output description, since that was what was partly used to determine if output should be taking place.  So it may be necessary for this method to decode
  * an output description string in order to determine what should be done.
  *@param documentURI is the URI of the document.  The URI is presumed to be the unique identifier which the output data store will use to process
  * and serve the document.  This URI is constructed by the repository connector which fetches the document, and is thus universal across all output connectors.
  *@param outputDescription is the description string that was constructed for this document by the getOutputDescription() method.
  *@param document is the document data to be processed (handed to the output data store).
  *@param authorityNameString is the name of the authority responsible for authorizing any access tokens passed in with the repository document.  May be null.
  *@param activities is the handle to an object that the implementer of an output connector may use to perform operations, such as logging processing activity.
  *@return the document status (accepted or permanently rejected).
  */
  @Override
  public int addOrReplaceDocumentWithException(String documentURI, VersionContext outputDescription, RepositoryDocument document, String authorityNameString, IOutputAddActivity activities)
    throws ManifoldCFException, ServiceInterruption, IOException
  {
    // Establish a session
    getSession();
    activities.recordActivity(null,INGEST_ACTIVITY,new Long(document.getBinaryLength()),documentURI,"OK",null);
    return DOCUMENTSTATUS_ACCEPTED;
  }

  /** Remove a document using the connector.
  * Note that the last outputDescription is included, since it may be necessary for the connector to use such information to know how to properly remove the document.
  *@param documentURI is the URI of the document.  The URI is presumed to be the unique identifier which the output data store will use to process
  * and serve the document.  This URI is constructed by the repository connector which fetches the document, and is thus universal across all output connectors.
  *@param outputDescription is the last description string that was constructed for this document by the getOutputDescription() method above.
  *@param activities is the handle to an object that the implementer of an output connector may use to perform operations, such as logging processing activity.
  */
  @Override
  public void removeDocument(String documentURI, String outputDescription, IOutputRemoveActivity activities)
    throws ManifoldCFException, ServiceInterruption
  {
    // Establish a session
    getSession();
    activities.recordActivity(null,REMOVE_ACTIVITY,null,documentURI,"OK",null);
  }

  /** Notify the connector of a completed job.
  * This is meant to allow the connector to flush any internal data structures it has been keeping around, or to tell the output repository that this
  * is a good time to synchronize things.  It is called whenever a job is either completed or aborted.
  *@param activities is the handle to an object that the implementer of an output connector may use to perform operations, such as logging processing activity.
  */
  @Override
  public void noteJobComplete(IOutputNotifyActivity activities)
    throws ManifoldCFException, ServiceInterruption
  {
    activities.recordActivity(null,JOB_COMPLETE_ACTIVITY,null,"","OK",null);
  }

}
