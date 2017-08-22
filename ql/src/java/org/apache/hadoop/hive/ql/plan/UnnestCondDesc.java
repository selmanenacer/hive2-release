/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedHashMap;


/**
 * Unnest conditions Descriptor implementation.
 * 
 */
public class UnnestCondDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private int left;
  private int right;
  private int type;
  protected final static Logger LOG = LoggerFactory.getLogger(UnnestCondDesc.class.getName());

  public UnnestCondDesc() {
  }

  public UnnestCondDesc(int left, int right, int type) {
    this.left = left;
    this.right = right;
    this.type = type;
  }

  public UnnestCondDesc(org.apache.hadoop.hive.ql.parse.UnnestCond condn) {
    left = condn.getLeft();
    switch (condn.getUnnestType()) {
    case INNER:
      type = UnnestJoinDesc.INNER_UNNEST;
      break;
    case LEFTOUTER:
      type = UnnestJoinDesc.LEFT_OUTER_UNNEST;
      break;
    default:
      assert false;
    }
  }

  public int getLeft() {
    return left;
  }

  public void setLeft(final int left) {
    this.left = left;
  }

  public int getRight() {
    return right;
  }

  public void setRight(final int right) {
    this.right = right;
  }

  public int getType() {
    return type;
  }

  public void setType(final int type) {
    this.type = type;
  }

  @Explain(explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public String getUnnestCondString() {
    StringBuilder sb = new StringBuilder();

    switch (type) {
    case UnnestJoinDesc.INNER_UNNEST:
      sb.append("Inner Unnest ");
      break;
    case UnnestJoinDesc.LEFT_OUTER_UNNEST:
      sb.append("Left Outer Unnest");
      break;
    default:
      sb.append("Unknow Unnest ");
      break;
    }

    sb.append(left);
    sb.append(" to ");
    sb.append(right);

    return sb.toString();
  }

  @Explain(explainLevels = { Level.USER })
  public String getUserLevelUnnestCondString() {
    JSONObject unnest = new JSONObject(new LinkedHashMap());
    try {
      switch (type) {
      case UnnestJoinDesc.INNER_UNNEST:
        unnest.put("type", "Inner");
        break;
      case UnnestJoinDesc.LEFT_OUTER_UNNEST:
        unnest.put("type", "Left Outer");
        break;
      default:
        unnest.put("type", "Unknow Unnest");
        break;
      }
      unnest.put("left", left);
      unnest.put("right", right);
    } catch (JSONException e) {
      // impossible to throw any json exceptions.
      LOG.trace(e.getMessage());
    }
    return unnest.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof UnnestCondDesc)) {
      return false;
    }

    UnnestCondDesc other = (UnnestCondDesc) obj;
    if (this.type != other.type || this.left != other.left ||
      this.right != other.right) {
      return false;
    }
    return true;
  }
}
