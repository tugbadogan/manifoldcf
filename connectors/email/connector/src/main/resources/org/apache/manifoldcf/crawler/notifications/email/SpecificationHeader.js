<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

<script type="text/javascript">
<!--

function s${SeqNum}_checkSpecificationForSave()
{
  if (s${SeqNum}_checkMessageTabForSave() == false)
    return false;
  return true;
}

function s${SeqNum}_SpecOp(n, opValue, anchorvalue)
{
  eval("editjob."+n+".value = \""+opValue+"\"");
  postFormSetAnchor(anchorvalue);
}

function s${SeqNum}_checkMessageTabForSave()
{
  if (editjob.s${SeqNum}_to.value == "")
  {
    alert("$Encoder.bodyJavascriptEscape($ResourceBundle.getString('EmailConnector.ToFieldCannotBeBlank'))");
    SelectSequencedTab("$Encoder.attributeJavascriptEscape($ResourceBundle.getString('EmailConnector.Message'))",${SeqNum})
    editjob.s${SeqNum}_to.focus();
    return false;
  }
  if (editjob.s${SeqNum}_from.value == "")
  {
    alert("$Encoder.bodyJavascriptEscape($ResourceBundle.getString('EmailConnector.FromFieldCannotBeBlank'))");
    SelectSequencedTab("$Encoder.attributeJavascriptEscape($ResourceBundle.getString('EmailConnector.Message'))",${SeqNum})
    editjob.s${SeqNum}_from.focus();
    return false;
  }

  return true;
}

//-->
</script>
