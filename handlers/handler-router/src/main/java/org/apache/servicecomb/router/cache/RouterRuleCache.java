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

package org.apache.servicecomb.router.cache;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.servicecomb.config.YAMLUtil;
import org.apache.servicecomb.router.model.PolicyRuleItem;
import org.apache.servicecomb.router.model.ServiceInfoCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

/**
 * @Author GuoYl123
 * @Date 2019/10/17
 * a tool class for reading rule and save to cache
 **/
public class RouterRuleCache {

  private static final Logger LOGGER = LoggerFactory.getLogger(RouterRuleCache.class);

  private static ConcurrentHashMap<String, ServiceInfoCache> serviceInfoCacheMap = new ConcurrentHashMap<>();

  private static final String ROUTE_RULE = "servicecomb.routeRule.%s";

  private static Interner<String> servicePool = Interners.newWeakInterner();

  /**
   * @param targetServiceName targetServiceName
   * @return false when parsing error or rule not exist
   */
  public static boolean getGrayScaleRuleForService(String targetServiceName) {
    if (!isServerContainRule(targetServiceName)) {
      return false;
    }
    // thread safe adding config
    if (!serviceInfoCacheMap.containsKey(targetServiceName)) {
      synchronized (servicePool.intern(targetServiceName)) {
        if (serviceInfoCacheMap.containsKey(targetServiceName)) {
          return true;
        }
        DynamicStringProperty ruleStr = DynamicPropertyFactory.getInstance().getStringProperty(
            String.format(ROUTE_RULE, targetServiceName), null, () -> {
              // handler when config changed
              refreshCache(targetServiceName);
              DynamicStringProperty tepRuleStr = DynamicPropertyFactory.getInstance()
                  .getStringProperty(String.format(ROUTE_RULE, targetServiceName), null);
              addAllRule(targetServiceName, tepRuleStr.get());
            });
        return addAllRule(targetServiceName, ruleStr.get());
      }
    }
    return true;
  }

  /**
   * @param targetServiceName
   * @param ruleStr
   * @return false when add rule failed
   */
  private static boolean addAllRule(String targetServiceName, String ruleStr) {
    if (StringUtils.isEmpty(ruleStr)) {
      return false;
    }
    List<PolicyRuleItem> policyRuleItemList;
    try {
      policyRuleItemList = Arrays
          .asList(YAMLUtil.parserObject(ruleStr, PolicyRuleItem[].class));
    } catch (Exception e) {
      LOGGER.error("route management parsing failed: {}", e.getMessage());
      return false;
    }
    if (CollectionUtils.isEmpty(policyRuleItemList)) {
      return false;
    }
    ServiceInfoCache serviceInfoCache = new ServiceInfoCache(policyRuleItemList);
    serviceInfoCacheMap.put(targetServiceName, serviceInfoCache);
    return true;
  }

  /**
   * check if a config does not contain rule for targetService, return
   * @param targetServiceName
   * @return false to avoid adding too many config-change-callback
   */
  public static boolean isServerContainRule(String targetServiceName) {
    DynamicStringProperty lookFor = DynamicPropertyFactory.getInstance()
        .getStringProperty(String.format(ROUTE_RULE, targetServiceName), null);
    return !StringUtils.isEmpty(lookFor.get());
  }

  public static ConcurrentHashMap<String, ServiceInfoCache> getServiceInfoCacheMap() {
    return serviceInfoCacheMap;
  }

  public static void refreshCache() {
    serviceInfoCacheMap = new ConcurrentHashMap<>();
  }

  public static void refreshCache(String targetServiceName) {
    serviceInfoCacheMap.remove(targetServiceName);
  }
}
