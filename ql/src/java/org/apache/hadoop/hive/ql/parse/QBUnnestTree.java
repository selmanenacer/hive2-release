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

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import java.io.Serializable;
import java.util.*;

/**
 * Internal representation of the unnest tree.
 *
 */
public class QBUnnestTree implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;
  private String leftAlias;
  private String[] rightAliases;
  private String[] leftAliases;
  private QBUnnestTree unnestSrc;
  private String[] baseSrc;
  private int nextTag;
  private UnnestCond[] unnestCond;
  private boolean noOuterUnnest;
  private Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo;
  private Map<String, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>> aliasToCondOpInfo;

  // The subquery identifier from QB.
  // It is of the form topSubQuery:innerSubQuery:....:innerMostSubQuery
  private String id;

  // unnest conditions
  private transient ArrayList<ArrayList<ASTNode>> expressions;

  // key index to nullsafe unnest flag
  private ArrayList<Boolean> nullsafes;

  // filters
  private transient ArrayList<ArrayList<ASTNode>> filters;

  // filters for pushing
  private transient ArrayList<ArrayList<ASTNode>> filtersForPushing;

  /*
   * when a QBUnnestTree is merged into this one, its left(pos =0) filters can
   * refer to any of the srces in this QBUnnestTree. If a particular filterForPushing refers
   * to multiple srces in this QBUnnestTree, we collect them into 'postUnnestFilters'
   * We then add a Filter Operator after the Unnest Operator for this QBUnnestTree.
   */
  private final List<ASTNode> postUnnestFilters;

  /**
   * constructor.
   */
  public QBUnnestTree() {
    nextTag = 0;
    noOuterUnnest = true;
    aliasToOpInfo = new HashMap<String, Operator<? extends OperatorDesc>>();
    aliasToCondOpInfo = new HashMap<String, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>>();
    postUnnestFilters = new ArrayList<ASTNode>();
  }

  /**
   * returns left alias if any - this is used for merging later on.
   *
   * @return left alias if any
   */
  public String getLeftAlias() {
    return leftAlias;
  }

  /**
   * set left alias for the unnest expression.
   *
   * @param leftAlias
   *          String
   */
  public void setLeftAlias(String leftAlias) {
    if ( this.leftAlias != null && !this.leftAlias.equals(leftAlias) ) {
      this.leftAlias = null;
    } else {
      this.leftAlias = leftAlias;
    }
  }

  public String[] getRightAliases() {
    return rightAliases;
  }

  public void setRightAliases(String[] rightAliases) {
    this.rightAliases = rightAliases;
  }

  public String[] getLeftAliases() {
    return leftAliases;
  }

  public void setLeftAliases(String[] leftAliases) {
    this.leftAliases = leftAliases;
  }

  public ArrayList<ArrayList<ASTNode>> getExpressions() {
    return expressions;
  }

  public void setExpressions(ArrayList<ArrayList<ASTNode>> expressions) {
    this.expressions = expressions;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public QBUnnestTree getUnnestSrc() {
    return unnestSrc;
  }

  public void setUnnestSrc(QBUnnestTree unnestSrc) {
    this.unnestSrc = unnestSrc;
  }

  public int getNextTag() {
    return nextTag++;
  }

  public UnnestCond[] getUnnestCond() {
    return unnestCond;
  }

  public void setUnnestCond(UnnestCond[] unnestCond) {
    this.unnestCond = unnestCond;
  }

  public boolean getNoOuterUnnest() {
    return noOuterUnnest;
  }

  public void setNoOuterUnnest(boolean noOuterUnnest) {
    this.noOuterUnnest = noOuterUnnest;
  }

  /**
   * @return the filters
   */
  public ArrayList<ArrayList<ASTNode>> getFilters() {
    return filters;
  }

  /**
   * @param filters
   *          the filters to set
   */
  public void setFilters(ArrayList<ArrayList<ASTNode>> filters) {
    this.filters = filters;
  }

  /**
   * @return the filters for pushing
   */
  public ArrayList<ArrayList<ASTNode>> getFiltersForPushing() {
    return filtersForPushing;
  }

  /**
   * @param filters for pushing
   *          the filters to set
   */
  public void setFiltersForPushing(ArrayList<ArrayList<ASTNode>> filters) {
    this.filtersForPushing = filters;
  }

  public ArrayList<Boolean> getNullSafes() {
    return nullsafes;
  }

  public void setNullSafes(ArrayList<Boolean> nullSafes) {
    this.nullsafes = nullSafes;
  }

  public Map<String, Operator<? extends OperatorDesc>> getAliasToOpInfo() {
    return aliasToOpInfo;
  }

  public void setAliasToOpInfo(Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo) {
    this.aliasToOpInfo = aliasToOpInfo;
  }

  public Map<String, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>> getAliasToCondOpInfo() {
    return aliasToCondOpInfo;
  }

  public void setAliasToCondOpInfo(Map<String, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>> aliasToCondOpInfo) {
    this.aliasToCondOpInfo = aliasToCondOpInfo;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void addPostUnnestFilter(ASTNode filter) {
    postUnnestFilters.add(filter);
  }

  public List<ASTNode> getPostUnnestFilters() {
    return postUnnestFilters;
  }

  @Override
  public QBUnnestTree clone() throws CloneNotSupportedException {
    QBUnnestTree cloned = new QBUnnestTree();

    // shallow copy aliasToOpInfo, we won't want to clone the operator tree here
    cloned.setAliasToOpInfo(aliasToOpInfo == null ? null :
            new HashMap<String, Operator<? extends OperatorDesc>>(aliasToOpInfo));

    // shallow copy aliasToCondOpInfo, we won't want to clone the operator tree here
    cloned.setAliasToCondOpInfo(aliasToCondOpInfo == null ? null :
            new HashMap<String, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>>(aliasToCondOpInfo));

    cloned.setBaseSrc(baseSrc == null ? null : baseSrc.clone());

    // shallow copy ASTNode
    cloned.setExpressions(expressions);
    cloned.setFilters(filters);
    cloned.setFiltersForPushing(filtersForPushing);

    cloned.setId(id);

    // clone unnestCond
    UnnestCond[] clonedUnnestCond = unnestCond == null ? null : new UnnestCond[unnestCond.length];
    if (unnestCond != null) {
      for (int i = 0; i < unnestCond.length; i++) {
        if(unnestCond[i] == null) {
          continue;
        }
        UnnestCond clonedCond = new UnnestCond();
        clonedCond.setUnnestType(unnestCond[i].getUnnestType());
        clonedCond.setLeft(unnestCond[i].getLeft());
        //clonedCond.setPreserved(unnestCond[i].getPreserved());
        clonedCond.setRight(unnestCond[i].getRight());
        clonedUnnestCond[i] = clonedCond;
      }
    }
    cloned.setUnnestCond(clonedUnnestCond);

    cloned.setUnnestSrc(unnestSrc == null ? null : unnestSrc.clone());
    cloned.setLeftAlias(leftAlias);
    cloned.setLeftAliases(leftAliases == null ? null : leftAliases.clone());
    cloned.setNoOuterUnnest(noOuterUnnest);
    cloned.setNullSafes(nullsafes == null ? null : new ArrayList<Boolean>(nullsafes));
    cloned.setRightAliases(rightAliases == null ? null : rightAliases.clone());

    // clone postUnnestFilters
    for (ASTNode filter : postUnnestFilters) {
      cloned.addPostUnnestFilter(filter);
    }

    return cloned;
  }


  public boolean hasFilter(){
    for(int i=0; i <this.getFiltersForPushing().size(); i++){
      ArrayList<ASTNode> filter = this.getFiltersForPushing().get(i);
      for (ASTNode cond : filter) {
        return true;
      }
    }
    return false;
  }

  public boolean hasExpression(){
    for(int i=0; i <this.getExpressions().size(); i++){
      ArrayList<ASTNode> filter = this.getExpressions().get(i);
      for (ASTNode cond : filter) {
        return true;
      }
    }
    return false;
  }

  public List<QBUnnestTree> getQBUnnestTreeList(List<QBUnnestTree> liste){
    QBUnnestTree unnestSrc = this.getUnnestSrc();
    if(!(unnestSrc == null)){
      unnestSrc.getQBUnnestTreeList(liste);
    }
    liste.add(this);
    return liste;
  }
}
