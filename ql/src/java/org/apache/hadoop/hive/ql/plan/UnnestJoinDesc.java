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

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import java.util.ArrayList;
import java.util.Map;


/**
 * UnnestJoinDesc.
 *
 */
@Explain(displayName = "Unnest Join Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class UnnestJoinDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private int numSelColumns;
  private ArrayList<String> outputInternalColNames;

  public UnnestJoinDesc() {
  }

  public UnnestJoinDesc(int numSelColumns, ArrayList<String> outputInternalColNames) {
    this.numSelColumns = numSelColumns;
    this.outputInternalColNames = outputInternalColNames;
  }

  public void setOutputInternalColNames(ArrayList<String> outputInternalColNames) {
    this.outputInternalColNames = outputInternalColNames;
  }

  @Explain(displayName = "outputColumnNames")
  public ArrayList<String> getOutputInternalColNames() {
    return outputInternalColNames;
  }

  @Explain(displayName = "Output", explainLevels = { Level.USER })
  public ArrayList<String> getUserLevelExplainOutputInternalColNames() {
    return outputInternalColNames;
  }

  public int getNumSelColumns() {
    return numSelColumns;
  }

  public void setNumSelColumns(int numSelColumns) {
    this.numSelColumns = numSelColumns;
  }

  //from joinDesc
  public static final int INNER_UNNEST = 0;
  public static final int LEFT_OUTER_UNNEST = 1;

  // No outer unnest involved
  protected boolean noOuterUnnest;

  protected UnnestCondDesc[] conds;

  // unnest conditions
  private ExprNodeDesc[][] unnestKeys;

  // filters
  private ArrayList<ArrayList<ExprNodeDesc>> filters;

  // Data structures coming originally from QBUnnestTree
  private transient String leftAlias;
  private transient String[] leftAliases;
  private transient String[] rightAliases;
  private transient String[] baseSrc;
  private transient String id;

  private transient Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo;
  private transient boolean leftInputUnnest;

  public UnnestJoinDesc(int numSelColumns, ArrayList<String> outputInternalColNames,
                        final boolean noOuterUnnest,
                        final UnnestCondDesc[] conds,
                        ExprNodeDesc[][] unnestKeys,
                        ArrayList<ArrayList<ExprNodeDesc>> filters) {
    this.numSelColumns = numSelColumns;
    this.outputInternalColNames = outputInternalColNames;
    this.noOuterUnnest = noOuterUnnest;
    this.conds = conds;
    this.unnestKeys = unnestKeys;
    this.filters = filters;
  }

  public static int getInnerUnnest() {
    return INNER_UNNEST;
  }

  public static int getLeftOuterUnnest() {
    return LEFT_OUTER_UNNEST;
  }

  public boolean isNoOuterUnnest() {
    return noOuterUnnest;
  }

  public void setNoOuterUnnest(boolean noOuterUnnest) {
    this.noOuterUnnest = noOuterUnnest;
  }

  public UnnestCondDesc[] getConds() {
    return conds;
  }

  public void setConds(UnnestCondDesc[] conds) {
    this.conds = conds;
  }

  public ExprNodeDesc[][] getUnnestKeys() {
    return unnestKeys;
  }

  public void setUnnestKeys(ExprNodeDesc[][] unnestKeys) {
    this.unnestKeys = unnestKeys;
  }

  public ArrayList<ArrayList<ExprNodeDesc>> getFilters() {
    return filters;
  }

  public void setFilters(ArrayList<ArrayList<ExprNodeDesc>> filters) {
    this.filters = filters;
  }

  public String getLeftAlias() {
    return leftAlias;
  }

  public void setLeftAlias(String leftAlias) {
    this.leftAlias = leftAlias;
  }

  public String[] getLeftAliases() {
    return leftAliases;
  }

  public void setLeftAliases(String[] leftAliases) {
    this.leftAliases = leftAliases;
  }

  public String[] getRightAliases() {
    return rightAliases;
  }

  public void setRightAliases(String[] rightAliases) {
    this.rightAliases = rightAliases;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Map<String, Operator<? extends OperatorDesc>> getAliasToOpInfo() {
    return aliasToOpInfo;
  }

  public void setAliasToOpInfo(Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo) {
    this.aliasToOpInfo = aliasToOpInfo;
  }

  public boolean isLeftInputUnnest() {
    return leftInputUnnest;
  }

  public void setLeftInputUnnest(boolean leftInputUnnest) {
    this.leftInputUnnest = leftInputUnnest;
  }
}
