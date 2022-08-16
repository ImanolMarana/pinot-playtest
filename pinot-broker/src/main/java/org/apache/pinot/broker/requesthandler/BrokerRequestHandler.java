/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.broker.api.RequesterIdentity;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.sql.parsers.SqlNodeAndOptions;


@ThreadSafe
public interface BrokerRequestHandler {

  void start();

  void shutDown();

  BrokerResponseNative handleRequest(JsonNode request, @Nullable SqlNodeAndOptions sqlNodeAndOptions,
      @Nullable RequesterIdentity requesterIdentity, RequestContext requestContext)
      throws Exception;

  default BrokerResponseNative handleRequest(JsonNode request, @Nullable RequesterIdentity requesterIdentity,
      RequestContext requestContext)
      throws Exception {
    return handleRequest(request, null, requesterIdentity, requestContext);
  }
}
