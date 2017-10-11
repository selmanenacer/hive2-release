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

package org.apache.hadoop.hive.ql.parse;

/**
 * Unnest conditions Descriptor implementation.
 * 
 */
public class UnnestCond {
  private int left;
  private int right;
  private UnnestType unnestType;


  public UnnestCond() {
  }

  public UnnestCond(int left, int right, UnnestType unnestType) {
    this.left = left;
    this.right = right;
    this.unnestType = unnestType;
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

  public UnnestType getUnnestType() {
    return unnestType;
  }

  public void setUnnestType(UnnestType unnestType) {
    this.unnestType = unnestType;
  }


}
