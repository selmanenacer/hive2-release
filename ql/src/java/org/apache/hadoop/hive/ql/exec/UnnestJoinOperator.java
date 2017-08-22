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

package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.UnnestJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.UnnestCollector;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.util.*;

/**
 * The unnest join operator is used for FROM src UNNEST udtf()...
 * This operator was implemented with the following operator DAG in mind.
 *
 * For a query such as
 *
 * SELECT pageid, adid.* FROM example_table UNNEST adid_list on (conditions)AS adid
 *
 * The top of the operator DAG will look similar to
 *
 *            [Table Scan]
 *                |
 *       [Unnest Forward]
 *              /   \
 *   [Select](*)    [Select](adid_list (Array))
 *            |      |
 *            \     /
 *      [Unnest Join]
 *               |
 *               |
 *      [Select] (pageid, adid.*)
 *               |
 *              ....
 *
 * Rows from the table scan operator are first to a unnest forward
 * operator that just forwards the row and marks the start of a Unnest join opertor. The
 * select operator on the left picks all the columns while the select operator
 * on the right picks only the columns to unnest.
 *
 * The output of select in the left branch and output of the sel in the right
 * branch are then sent to the unnest join (UJ). In most cases, the sel in right side
 * will generate > 1 row for every row received from the TS, while the left
 * select operator will generate only one. For each row output from the TS, the
 * UJ outputs rows that can be created by joining the row that verify join conditions on the On clause from the
 * left select and one of the rows output from the right select.
 *
 * Additional unnests can be supported by adding a similar DAG after the
 * previous UJ operator.
 */

public class UnnestJoinOperator extends Operator<UnnestJoinDesc> {

  private static final long serialVersionUID = 1L;

  // The expected tags from the parent operators. See processOp() before
  // changing the tags.
  public static final byte SELECT_TAG = 0;
  public static final byte UDTF_TAG = 1;

  Object[] objToSendToUnnest = null;
  GenericUDTF genericUDTF = null;
  UnnestCollector collector = null;
  List outerObj;
  int [][] unnestKeysId;
  ArrayListValuedHashMap multiValuedMap;
  long currentBlockStart;
  boolean selectObjsMatchAtLessOnce;

  /** Kryo ctor. */
  protected UnnestJoinOperator() {
    super();
  }

  public UnnestJoinOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    ArrayList<String> fieldNames = conf.getOutputInternalColNames();

    // The output of the unnest join will be the columns from the select
    // parent, followed by the column from the UDTF parent
    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[SELECT_TAG];

    List<? extends StructField> sfs = soi.getAllStructFieldRefs();
    for (StructField sf : sfs) {
      ois.add(sf.getFieldObjectInspector());
    }

    String funcName = "inline";
    FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
    genericUDTF = fi.getGenericUDTF();
    collector = new UnnestCollector(this);
    genericUDTF.setCollector(collector);
    objToSendToUnnest = new Object[((StructObjectInspector) inputObjInspectors[UDTF_TAG]).getAllStructFieldRefs().size()];
    soi = genericUDTF.initialize((StructObjectInspector) inputObjInspectors[UDTF_TAG]);
    if (!conf.isNoOuterUnnest()) {
      outerObj = Arrays.asList(new Object[soi.getAllStructFieldRefs().size()]);
    }

    sfs = soi.getAllStructFieldRefs();
    for (StructField sf : sfs) {
      ois.add(sf.getFieldObjectInspector());
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, ois);
    initUnnestKeysId();
    multiValuedMap = null;
    currentBlockStart = -1;
    selectObjsMatchAtLessOnce = false;
  }

  // acc is short for accumulator. It's used to build the row before forwarding
  ArrayList<Object> acc = new ArrayList<Object>();
  // selectObjs hold the row from the select op, until receiving a row from
  // the udtf op
  ArrayList<Object> selectObjs = new ArrayList<Object>();

  /**
   * An important assumption for processOp() is that for a given row from the
   * TS, the LVJ will first get the row from the left select operator, followed
   * by all the corresponding rows from the UDTF operator. And so on.
   */
  @Override
  public void process(Object row, int tag) throws HiveException {
    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[tag];
    if (tag == SELECT_TAG) {
      selectObjs.clear();
      selectObjs.addAll(soi.getStructFieldsDataAsList(row));
    } else if (tag == UDTF_TAG) {
      if(multiValuedMap == null){
        // First call or I dont have join conditions
        processUnnestOutput(row, UDTF_TAG);
      } else if(currentBlockStart != this.getExecContext().getIoCxt().getCurrentBlockStart()){
        // I have join conditions but I change the Row, so I must clear the Index and compute another one
        multiValuedMap.clear();
        currentBlockStart = this.getExecContext().getIoCxt().getCurrentBlockStart();
        processUnnestOutput(row, UDTF_TAG);
      } else {
        // I have join conditions and I change the Row, so I use the Index
        StructObjectInspector soiField = genericUDTF.initialize((StructObjectInspector) inputObjInspectors[UDTF_TAG]);
        List l = multiValuedMap.get(selectObjs.get(unnestKeysId[0][0]));
        if (l == null && !conf.isNoOuterUnnest()) {
          acc.clear();
          acc.addAll(selectObjs);
          acc.addAll(soi.getStructFieldsDataAsList(outerObj));
          forward(acc, outputObjInspector);
        } else {
          boolean selectObjsMatchAtLessOnce = false;
          for (Object o : l) {
            acc.clear();
            acc.addAll(selectObjs);
            acc.addAll(soiField.getStructFieldsDataAsList(o));
            boolean match = true;
            for (int j = 1; match && j < unnestKeysId[0].length; j++) {
              if(!acc.get(unnestKeysId[0][j]).equals(acc.get(unnestKeysId[1][j]))){
                match = false;
              }
            }
            if(match){
              selectObjsMatchAtLessOnce = true;
              forward(acc, outputObjInspector);
            }
          }
          if(!selectObjsMatchAtLessOnce && !conf.isNoOuterUnnest()){
            acc.clear();
            acc.addAll(selectObjs);
            acc.addAll(soi.getStructFieldsDataAsList(outerObj));
            forward(acc, outputObjInspector);
          }
        }
      }
    } else {
      throw new HiveException("Invalid tag");
    }

  }

  @Override
  public String getName() {
    return UnnestJoinOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "UJ";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.UNNESTJOIN;
  }

  /**
   * forwardUnnestOutput is typically called indirectly by the UJ to
   * generated output rows that should be passed on to the
   * next operator(s) in the DAG.
   *
   * @param o
   * @throws HiveException
   */
  public void forwardUnnestOutput(Object o) throws HiveException {
    // Since the output of the UDTF is a struct, we can just forward that
    StructObjectInspector soi = genericUDTF.initialize((StructObjectInspector) inputObjInspectors[UDTF_TAG]);
    acc.clear();
    acc.addAll(selectObjs);
    acc.addAll(soi.getStructFieldsDataAsList(o));
    //check unnest conditions
    if(unnestKeysId[0].length != 0){
      multiValuedMap.put(acc.get(unnestKeysId[1][0]),o);
      for(int j=0; j<unnestKeysId[0].length; j++){
        if(!acc.get(unnestKeysId[0][j]).equals(acc.get(unnestKeysId[1][j]))){
          return;
        }
      }
    }
    selectObjsMatchAtLessOnce = true;
    forward(acc, outputObjInspector);
  }

  public void processUnnestOutput(Object row, int tag) throws HiveException {
    // The UDTF expects arguments in an object[]
    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[tag];
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i = 0; i < fields.size(); i++) {
      objToSendToUnnest[i] = soi.getStructFieldData(row, fields.get(i));
    }

    // init currentBlockStart and multiValuedMap
    if(unnestKeysId[0].length != 0 && multiValuedMap == null){
      int size = getArraySize();
      multiValuedMap = new ArrayListValuedHashMap<Object, ArrayList<Object>>(size,1);
      currentBlockStart = this.getExecContext().getIoCxt().getCurrentBlockStart();
    }

    selectObjsMatchAtLessOnce = false;
    genericUDTF.process(objToSendToUnnest);
    if(!selectObjsMatchAtLessOnce && !conf.isNoOuterUnnest()){
      acc.clear();
      acc.addAll(selectObjs);
      acc.addAll(soi.getStructFieldsDataAsList(outerObj));
      forward(acc, outputObjInspector);
    }
    collector.reset();
  }

  public void initUnnestKeysId(){
    ExprNodeDesc[][] unnestKeys = this.getConf().getUnnestKeys();
    unnestKeysId = new int[unnestKeys.length][unnestKeys[0].length];
    for(int j=0; j<unnestKeys[0].length; j++) {
      String leftVal = unnestKeys[0][j].toString();
      String rightVal = unnestKeys[1][j].toString();
      Iterator<Map.Entry<String, ExprNodeDesc>> iters = colExprMap.entrySet().iterator();
      Boolean LeftNotFound = true;
      Boolean rightNotFound = true;
      int leftId = -1;
      int rightId = -1;
      while (iters.hasNext() && (LeftNotFound || rightNotFound)) {
        Map.Entry<String, ExprNodeDesc> entry = iters.next();
        String val = (entry.getValue()).toString();
        String key = entry.getKey();
        if (val.equals(leftVal) && LeftNotFound) {
          LeftNotFound = false;
          leftId = ((StructObjectInspector) outputObjInspector).getStructFieldRef(key).getFieldID();
        }
        if (val.equals(rightVal) && rightNotFound) {
          rightNotFound = false;
          rightId = ((StructObjectInspector) outputObjInspector).getStructFieldRef(key).getFieldID();
        }
      }
      unnestKeysId[0][j] = leftId;
      unnestKeysId[1][j] = rightId;
    }
  }

  int getArraySize(){
    List<? extends StructField> inputFields = ((StructObjectInspector) inputObjInspectors[UDTF_TAG]).getAllStructFieldRefs();
    ObjectInspector[] udtfInputOIs = new ObjectInspector[inputFields.size()];
    for (int i = 0; i < inputFields.size(); i++) {
      udtfInputOIs[i] = inputFields.get(i).getFieldObjectInspector();
    }
    ListObjectInspector li = (ListObjectInspector) udtfInputOIs[0];
    li.getList(objToSendToUnnest[0]).size();
    return li.getList(objToSendToUnnest[0]).size();
  }

}
