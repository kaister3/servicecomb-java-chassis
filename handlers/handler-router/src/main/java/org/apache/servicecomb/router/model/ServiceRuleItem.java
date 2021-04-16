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
package org.apache.servicecomb.router.model;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author GuoYl123
 * @Date 2019/10/17
 * every serviceName has a ServiceInfoCache
 **/
public class ServiceRuleItem {
  private List<PrecedenceRuleItem> precedenceRuleItems;

  /**
   * each service has a latest version
   */
  private TagItem latestVersionTag;

  public ServiceRuleItem(List<PrecedenceRuleItem> precedenceRuleItemList) {
    this.precedenceRuleItems = precedenceRuleItemList;
    // init each tagItem of each routeItems
    this.precedenceRuleItems.forEach(rule -> rule.getRouteItems().forEach(RouteItem::initTagItem));
    // sort by precedence
    this.sortRule();
  }

  public void sortRule() {
    precedenceRuleItems = precedenceRuleItems.stream().sorted().collect(Collectors.toList());
  }

  /**
   * @return the current max weight tagItem
   */
  public TagItem getNextInvokeVersion(PrecedenceRuleItem precedenceRuleItem) {
    List<RouteItem> routeItems = precedenceRuleItem.getRouteItems();
    if (precedenceRuleItem.getTotalWeight() == null) {
      precedenceRuleItem.setTotalWeight(routeItems.stream().mapToInt(RouteItem::getWeight).sum());
    }
    for (RouteItem routeItem : routeItems) {
      routeItem.initTagItem();
    }
    int maxIndex = 0, maxWeight = -1;
    for (int i = 0; i < routeItems.size(); i++) {
      if (maxWeight < routeItems.get(i).getCurrentWeight()) {
        maxIndex = i;
        maxWeight = routeItems.get(i).getCurrentWeight();
      }
    }
    routeItems.get(maxIndex).reduceCurrentWeight(precedenceRuleItem.getTotalWeight());
    return routeItems.get(maxIndex).getTagitem();
  }

  public TagItem getLatestVersionTag() {
    return latestVersionTag;
  }

  public void setLatestVersionTag(TagItem latestVersionTag) {
    this.latestVersionTag = latestVersionTag;
  }

  public List<PrecedenceRuleItem> getPrecedenceRuleItems() {
    return precedenceRuleItems;
  }
}
