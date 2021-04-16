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

import org.apache.servicecomb.router.exception.RouterIllegalParamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author GuoYl123
 * @Date 2019/10/17
 **/
public class HeaderRuleItem {
  private static final Logger LOGGER = LoggerFactory.getLogger(HeaderRuleItem.class);

  public final static String REGEX_PROPERTY = "regex";

  public final static  String EXACT_PROPERTY = "exact";

  private String regex;

  private String exact;

  /**
   * false distinct
   * true Ignore
   */
  private boolean caseInsensitive = false;

  public HeaderRuleItem() {}

  /**
   * check if the header matches argument regex and exact
   */
  public boolean match(String headerRegex, String headerExact) {
    if (exact == null && regex == null) {
      throw new RouterIllegalParamException(
          "route header argument regex and exact cannot be null at the same time!"
      );
    }
    boolean argumentExactIsMatched = false;
    if (exact != null) {
      if (headerExact != null) {
        if (!exact.equals(headerExact)) {
          return false;
        } else {
          argumentExactIsMatched = true;
        }
      }
    }

    if (!caseInsensitive) {
      headerRegex = headerRegex.toLowerCase();
      this.regex = regex.toLowerCase();
    }

    if (!argumentExactIsMatched) {
      if (regex == null || headerRegex == null) {
        return false;
      }
    }

    if (regex != null && !regex.matches(headerRegex)) {
      return false;
    }
    return true;
  }
}
