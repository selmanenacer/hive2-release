/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
parser grammar IdentifiersParser2;

options
{
output=AST;
ASTLabelType=ASTNode;
backtrack=false;
k=3;
}

@members {
  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }
  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    gParent.errors.add(new ParseError(gParent, e, tokenNames));
  }
  protected boolean useSQL11ReservedKeywordsForIdentifier() {
    return gParent.useSQL11ReservedKeywordsForIdentifier();
  }
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}

//-----------------------------------------------------------------------------------
// group by a,b
groupByClause
@init { gParent.pushMsg("group by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_GROUP KW_BY groupby_expression
    -> groupby_expression
    ;

// support for new and old rollup/cube syntax
groupby_expression :
 rollupStandard |
 rollupOldSyntax
;

// standard rollup syntax
rollupStandard
@init { gParent.pushMsg("standard rollup syntax", state); }
@after { gParent.popMsg(state); }
    :
    (rollup=KW_ROLLUP | cube=KW_CUBE)
    LPAREN expression ( COMMA expression)* RPAREN
    -> {rollup != null}? ^(TOK_ROLLUP_GROUPBY expression+)
    -> ^(TOK_CUBE_GROUPBY expression+)
    ;

// old hive rollup syntax
rollupOldSyntax
@init { gParent.pushMsg("rollup old syntax", state); }
@after { gParent.popMsg(state); }
    :
    expr=expressionsNotInParenthesis[false]
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE)) ?
    (sets=KW_GROUPING KW_SETS
    LPAREN groupingSetExpression ( COMMA groupingSetExpression)*  RPAREN ) ?
    -> {rollup != null}? ^(TOK_ROLLUP_GROUPBY {$expr.tree})
    -> {cube != null}? ^(TOK_CUBE_GROUPBY {$expr.tree})
    -> {sets != null}? ^(TOK_GROUPING_SETS {$expr.tree} groupingSetExpression+)
    -> ^(TOK_GROUPBY {$expr.tree})
    ;


groupingSetExpression
@init {gParent.pushMsg("grouping set expression", state); }
@after {gParent.popMsg(state); }
   :
   (groupingSetExpressionMultiple) => groupingSetExpressionMultiple
   |
   groupingExpressionSingle
   ;

groupingSetExpressionMultiple
@init {gParent.pushMsg("grouping set part expression", state); }
@after {gParent.popMsg(state); }
   :
   LPAREN
   expression? (COMMA expression)*
   RPAREN
   -> ^(TOK_GROUPING_SETS_EXPRESSION expression*)
   ;

groupingExpressionSingle
@init { gParent.pushMsg("groupingExpression expression", state); }
@after { gParent.popMsg(state); }
    :
    expression -> ^(TOK_GROUPING_SETS_EXPRESSION expression)
    ;

havingClause
@init { gParent.pushMsg("having clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_HAVING havingCondition -> ^(TOK_HAVING havingCondition)
    ;

havingCondition
@init { gParent.pushMsg("having condition", state); }
@after { gParent.popMsg(state); }
    :
    expression
    ;

expressionsInParenthesis[boolean isStruct]
    :
    LPAREN! expressionsNotInParenthesis[isStruct] RPAREN!
    ;

expressionsNotInParenthesis[boolean isStruct]
    :
    first=expression more=expressionPart[$expression.tree, isStruct]?
    -> {more==null}?
       {$first.tree}
    -> {$more.tree}
    ;

expressionPart[CommonTree t, boolean isStruct]
    :
    (COMMA expression)+
    -> {isStruct}? ^(TOK_FUNCTION Identifier["struct"] {$t} expression+)
    -> {$t} expression+
    ;

expressions
    :
    (expressionsInParenthesis[false]) => expressionsInParenthesis[false]
    |
    expressionsNotInParenthesis[false]
    ;

columnRefOrderInParenthesis
    :
    LPAREN columnRefOrder (COMMA columnRefOrder)* RPAREN -> columnRefOrder+
    ;

columnRefOrderNotInParenthesis
    :
    columnRefOrder (COMMA columnRefOrder)* -> columnRefOrder+
    ;

// order by a,b
orderByClause
@init { gParent.pushMsg("order by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_ORDER KW_BY columnRefOrder ( COMMA columnRefOrder)* -> ^(TOK_ORDERBY columnRefOrder+)
    ;

clusterByClause
@init { gParent.pushMsg("cluster by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_CLUSTER KW_BY expressions -> ^(TOK_CLUSTERBY expressions)
    ;

partitionByClause
@init  { gParent.pushMsg("partition by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_PARTITION KW_BY expressions -> ^(TOK_DISTRIBUTEBY expressions)
    ;

distributeByClause
@init { gParent.pushMsg("distribute by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_DISTRIBUTE KW_BY expressions -> ^(TOK_DISTRIBUTEBY expressions)
    ;

sortByClause
@init { gParent.pushMsg("sort by clause", state); }
@after { gParent.popMsg(state); }
    :
    KW_SORT KW_BY
    (
    (LPAREN) => columnRefOrderInParenthesis -> ^(TOK_SORTBY columnRefOrderInParenthesis)
    |
    columnRefOrderNotInParenthesis -> ^(TOK_SORTBY columnRefOrderNotInParenthesis)
    )
    ;