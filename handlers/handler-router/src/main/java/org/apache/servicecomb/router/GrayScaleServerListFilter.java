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

package org.apache.servicecomb.router;

import static org.apache.servicecomb.router.util.Constants.GRAY_SCALE_FLAG;
import static org.apache.servicecomb.router.util.Constants.ROUTER_HEADER;
import static org.apache.servicecomb.router.util.Constants.TYPE_ROUTER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.servicecomb.core.Invocation;
import org.apache.servicecomb.foundation.common.utils.JsonUtils;
import org.apache.servicecomb.foundation.common.utils.SPIServiceUtils;
import org.apache.servicecomb.loadbalance.ServerListFilterExt;
import org.apache.servicecomb.loadbalance.ServiceCombServer;
import org.apache.servicecomb.router.model.RouteRuleItem;
import org.apache.servicecomb.router.distributor.ServiceCombCanaryDistributer;
import org.apache.servicecomb.router.distributor.RouterDistributor;
import org.apache.servicecomb.registry.api.registry.Microservice;
import org.apache.servicecomb.router.filter.RouterHeaderFilterExt;
import org.apache.servicecomb.router.model.PrecedenceRuleItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.config.DynamicPropertyFactory;

public class GrayScaleServerListFilter implements ServerListFilterExt {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrayScaleServerListFilter.class);

  RouterDistributor<ServiceCombServer, Microservice> distributor = new ServiceCombCanaryDistributer();

  /**
   * @return if the gray scale is opened
   */
  @Override
  public boolean enabled() {
    return DynamicPropertyFactory.getInstance().getStringProperty(GRAY_SCALE_FLAG, "").get()
        .equals(TYPE_ROUTER);
  }

  /**
   * @param list server list
   * @param invocation invocation
   * @return the filtered server list
   */
  @Override
  public List<ServiceCombServer> getFilteredListOfServers(List<ServiceCombServer> list, Invocation invocation) {

    // 0.check if server list or targetServiceName is empty
    if (CollectionUtils.isEmpty(list)) {
      return list;
    }
    String targetServiceName = invocation.getMicroserviceName();
    if (StringUtils.isEmpty(targetServiceName)) {
      return list;
    }

    // 1.init cache
    if (!RouteRuleItem.getGrayScaleRuleForService(targetServiceName)) {
      LOGGER.debug("route management init failed");
      return list;
    }

    // 2.match rule
    Map<String, String> headers = filterHeaders(addHeaders(invocation));
    PrecedenceRuleItem invokeRule = RouteRuleItem.getInstance().matchHeader(targetServiceName, headers);

    if (invokeRule == null) {
      LOGGER.info("this invocation does not match any grayscale rule");
      return list;
    }
    LOGGER.info("route management match rule success: {}", invokeRule);

    // 3.distribute selected endpoint
    List<ServiceCombServer> resultList = distributor.distribute(targetServiceName, list, invokeRule);
    LOGGER.info("route management distribute rule success: {}", resultList);
    return resultList;
  }

  private Map<String, String> filterHeaders(Map<String, String> headers) {
    // users can add custom header filters
    // 可移到其他包里，与灰度发布关联小
    List<RouterHeaderFilterExt> filters = SPIServiceUtils
        .getOrLoadSortedService(RouterHeaderFilterExt.class);
    for (RouterHeaderFilterExt filterExt : filters) {
      if (filterExt.enabled()) {
        headers = filterExt.doFilter(headers);
      }
    }
    return headers;
  }

  private Map<String, String> addHeaders(Invocation invocation) {
    Map<String, String> headers = new HashMap<>();
    if (invocation.getContext(ROUTER_HEADER) != null) {
      Map<String, String> canaryContext = null;
      try {
        canaryContext = JsonUtils.OBJ_MAPPER
            .readValue(invocation.getContext(ROUTER_HEADER),
                new TypeReference<Map<String, String>>() {
                });
      } catch (JsonProcessingException e) {
        LOGGER.error("canary context serialization failed");
      }
      if (canaryContext != null) {
        headers.putAll(canaryContext);
      }
    }
    invocation.getInvocationArguments().forEach((k, v) -> headers.put(k, v == null ? null : v.toString()));
    headers.putAll(invocation.getContext());
    return headers;
  }
}
