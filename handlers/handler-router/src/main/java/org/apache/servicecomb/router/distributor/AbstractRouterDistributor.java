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

package org.apache.servicecomb.router.distributor;

import com.netflix.loadbalancer.Server;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.servicecomb.router.model.RouteRuleItem;
import org.apache.servicecomb.router.model.PrecedenceRuleItem;
import org.apache.servicecomb.router.model.RouteItem;
import org.apache.servicecomb.router.model.TagItem;
import org.apache.servicecomb.router.util.VersionCompareUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

/**
 * @Author GuoYl123
 * @Date 2019/10/17
 **/
public abstract class AbstractRouterDistributor<T extends Server, E> implements RouterDistributor<T, E> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRouterDistributor.class);

  private Function<T, E> getIns;

  private Function<E, String> getVersion;

  private Function<E, String> getServerName;

  private Function<E, Map<String, String>> getProperties;

  @Override
  public List<T> distribute(String targetServiceName, List<T> list, PrecedenceRuleItem invokeRule) {
    //init LatestVersion
    initLatestVersion(targetServiceName, list);

    invokeRule.check(
        RouteRuleItem.getServiceRuleItemMap().get(targetServiceName).getLatestVersionTag());

    // get tag list
    Map<TagItem, List<T>> versionServerMap = getDistributeList(targetServiceName, list, invokeRule);

    if (CollectionUtils.isEmpty(versionServerMap)) {
      LOGGER.debug("route management can not match any rule and route the latest version");
      return getLatestVersionList(list, targetServiceName);
    }

    TagItem targetTag = getFilteredServerTagItem(invokeRule, targetServiceName);
    if (versionServerMap.containsKey(targetTag)) {
      return versionServerMap.get(targetTag);
    }
    return getLatestVersionList(list, targetServiceName);
  }

  /**
   * init methods
   */
  @Override
  public void init(Function<T, E> getIns,
      Function<E, String> getVersion,
      Function<E, String> getServerName,
      Function<E, Map<String, String>> getProperties) {
    this.getIns = getIns;
    this.getVersion = getVersion;
    this.getServerName = getServerName;
    this.getProperties = getProperties;
  }

  public TagItem getFilteredServerTagItem(PrecedenceRuleItem rule, String targetServiceName) {
    return RouteRuleItem.getServiceRuleItemMap().get(targetServiceName).getLatestVersionTag();
  }

  /**
   * 1.filter targetService
   * 2.establish map in a more complicate way than direct traversal, because of multiple matches.
   * the method getProperties() contains other field that we don't need.
   */
  private Map<TagItem, List<T>> getDistributeList(String serviceName,
      List<T> list,
      PrecedenceRuleItem invokeRule) {
    String latestV = RouteRuleItem.getServiceRuleItemMap().get(serviceName).getLatestVersionTag()
        .getVersion();
    Map<TagItem, List<T>> versionServerMap = new HashMap<>();
    for (T server : list) {
      //get server
      E ms = getIns.apply(server);
      if (getServerName.apply(ms).equals(serviceName)) {
        //most matching
        TagItem tagItem = new TagItem(getVersion.apply(ms), getProperties.apply(ms));
        TagItem targetTag = null;
        int maxMatch = 0;
        for (RouteItem entry : invokeRule.getRouteItems()) {
          int nowMatch = entry.getTagitem().matchNum(tagItem);
          if (nowMatch > maxMatch) {
            maxMatch = nowMatch;
            targetTag = entry.getTagitem();
          }
        }
        if (invokeRule.isWeightLess() && getVersion.apply(ms).equals(latestV)) {
          TagItem latestVTag = invokeRule.getRouteItems().get(invokeRule.getRouteItems().size() - 1)
              .getTagitem();
          if (!versionServerMap.containsKey(latestVTag)) {
            versionServerMap.put(latestVTag, new ArrayList<>());
          }
          versionServerMap.get(latestVTag).add(server);
        }
        if (targetTag != null) {
          if (!versionServerMap.containsKey(targetTag)) {
            versionServerMap.put(targetTag, new ArrayList<>());
          }
          versionServerMap.get(targetTag).add(server);
        }
      }
    }
    return versionServerMap;
  }


  /**
   * init the latest version
   */
  public void initLatestVersion(String serviceName, List<T> list) {
    if (RouteRuleItem.getServiceRuleItemMap().get(serviceName).getLatestVersionTag() != null) {
      return;
    }
    String latestVersion = null;
    for (T server : list) {
      E microservice = getIns.apply(server);
      if (getServerName.apply(microservice).equals(serviceName)) {
        if (latestVersion == null || VersionCompareUtil
            .compareVersion(latestVersion, getVersion.apply(microservice)) == -1) {
          latestVersion = getVersion.apply(microservice);
        }
      }
    }
    TagItem tagitem = new TagItem(latestVersion);
    RouteRuleItem.getServiceRuleItemMap().get(serviceName).setLatestVersionTag(tagitem);
  }

  public List<T> getLatestVersionList(List<T> list, String targetServiceName) {
    String latestVersion = RouteRuleItem.getServiceRuleItemMap().get(targetServiceName)
        .getLatestVersionTag().getVersion();
    return list.stream().filter(server ->
        getVersion.apply(getIns.apply(server)).equals(latestVersion)
    ).collect(Collectors.toList());
  }
}
