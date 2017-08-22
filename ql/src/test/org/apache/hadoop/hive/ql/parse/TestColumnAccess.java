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

import java.io.IOException;
import java.util.*;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestColumnAccess {

  @BeforeClass
  public static void Setup() throws CommandNeedRetryException {
    Driver driver = createDriver();
    int ret = driver.run("create table t1(id1 int, name1 string)").getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);
    ret = driver.run("create table t2(id2 int, id1 int, name2 string)").getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);
    ret = driver.run("create view v1 as select * from t1").getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);
    String query = "create table car (" +
            "cars array<struct<" +
            "trademark_id_ref:int," +
            "reseller_id_ref:int," +
            "models:array<struct<" +
            "family:string," +
            "category:array<struct<name:string,quantity:int>>" +
            ">>" +
            ">>," +
            "trademarks array<struct<name:string,trademark_id:int>>," +
            "resellers array<struct<name:string,reseller_id:int>>" +
            ") stored as parquet";
    //"create table car (cars array<struct<trademark_id_ref:int,reseller_id_ref:int,models:array<struct<family:string,category:array<struct<name:string,quantity:int>>>>>>,trademarks array<struct<name:string,trademark_id:int>>,resellers array<struct<name:string,reseller_id:int>>) stored as parquet";
    ret = driver.run(query).getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);

    query = "create table dummy (x string)";
    ret = driver.run(query).getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);

    query = "insert into dummy (x) values ('toto')";
    ret = driver.run(query).getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);

    query = "insert into table car select\n" +
            "\n" +
            "array(\n" +
            "\n" +
            "named_struct('trademark_id_ref',1,'reseller_id_ref',1,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','FIESTA',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.2 GHIA','quantity',10),\n" +
            "\n" +
            "              named_struct('name','1.6 GHIA','quantity',20)\n" +
            "\n" +
            "        )),\n" +
            "\n" +
            "      named_struct('family','MONDEO',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.8 GHIA','quantity',3),\n" +
            "\n" +
            "              named_struct('name','2.0 GHIA','quantity',5)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "      )),\n" +
            "\n" +
            "named_struct('trademark_id_ref',1,'reseller_id_ref',2,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','FIESTA',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.2 GHIA','quantity',20),\n" +
            "\n" +
            "              named_struct('name','1.6 GHIA','quantity',40)\n" +
            "\n" +
            "        )),\n" +
            "\n" +
            "      named_struct('family','MONDEO',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.8 GHIA','quantity',1),\n" +
            "\n" +
            "              named_struct('name','2.0 GHIA','quantity',7)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "      )),\n" +
            "\n" +
            "named_struct('trademark_id_ref',1,'reseller_id_ref',3,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','FIESTA',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.2 GHIA','quantity',20),\n" +
            "\n" +
            "              named_struct('name','1.6 GHIA','quantity',40)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "    )),\n" +
            "\n" +
            "named_struct('trademark_id_ref',2,'reseller_id_ref',3,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','CLIO',\n" +
            "\n" +
            "        'category',array(named_struct('name','RS SPORT','quantity',20)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "      ))\n" +
            "\n" +
            ") as cars,\n" +
            "\n" +
            "array(\n" +
            "\n" +
            "  named_struct('name','FORD','trademark_id',1),\n" +
            "\n" +
            "  named_struct('name','RENAULT','trademark_id',2)\n" +
            "\n" +
            ")as trademarks,\n" +
            "\n" +
            "array(\n" +
            "\n" +
            "  named_struct('name','GARAGE PARIS','reseller_id',1),\n" +
            "\n" +
            "  named_struct('name','GARAGE LYON','reseller_id',2),\n" +
            "\n" +
            "  named_struct('name','GARAGE MARSEILLE','reseller_id',3)\n" +
            "\n" +
            ") as resellers\n" +
            "\n" +
            " \n" +
            "\n" +
            "from  dummy limit 1";
    ret = driver.run(query).getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);

    query = "insert into table car select\n" +
            "\n" +
            "array(\n" +
            "\n" +
            "named_struct('trademark_id_ref',1,'reseller_id_ref',1,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','FIESTA 1',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.2 GHIA','quantity',10),\n" +
            "\n" +
            "              named_struct('name','1.6 GHIA','quantity',20)\n" +
            "\n" +
            "        )),\n" +
            "\n" +
            "      named_struct('family','MONDEO 1',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.8 GHIA','quantity',3),\n" +
            "\n" +
            "              named_struct('name','2.0 GHIA','quantity',5)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "      )),\n" +
            "\n" +
            "named_struct('trademark_id_ref',1,'reseller_id_ref',2,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','FIESTA 1',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.2 GHIA','quantity',20),\n" +
            "\n" +
            "              named_struct('name','1.6 GHIA','quantity',40)\n" +
            "\n" +
            "        )),\n" +
            "\n" +
            "      named_struct('family','MONDEO 1',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.8 GHIA','quantity',1),\n" +
            "\n" +
            "              named_struct('name','2.0 GHIA','quantity',7)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "      )),\n" +
            "\n" +
            "named_struct('trademark_id_ref',1,'reseller_id_ref',3,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','FIESTA 1',\n" +
            "\n" +
            "        'category',array(named_struct('name','1.2 GHIA','quantity',20),\n" +
            "\n" +
            "              named_struct('name','1.6 GHIA','quantity',40)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "    )),\n" +
            "\n" +
            "named_struct('trademark_id_ref',2,'reseller_id_ref',3,\n" +
            "\n" +
            "    'models',array(\n" +
            "\n" +
            "      named_struct('family','CLIO 1',\n" +
            "\n" +
            "        'category',array(named_struct('name','RS SPORT','quantity',20)\n" +
            "\n" +
            "        ))\n" +
            "\n" +
            "      ))\n" +
            "\n" +
            ") as cars,\n" +
            "\n" +
            "array(\n" +
            "\n" +
            "  named_struct('name','FORD','trademark_id',1),\n" +
            "\n" +
            "  named_struct('name','RENAULT','trademark_id',2)\n" +
            "\n" +
            ")as trademarks,\n" +
            "\n" +
            "array(\n" +
            "\n" +
            "  named_struct('name','GARAGE PARIS 1','reseller_id',1),\n" +
            "\n" +
            "  named_struct('name','GARAGE LYON 1','reseller_id',2),\n" +
            "\n" +
            "  named_struct('name','GARAGE MARSEILLE 1','reseller_id',3)\n" +
            "\n" +
            ") as resellers\n" +
            "\n" +
            " \n" +
            "\n" +
            "from  dummy limit 1";
    ret = driver.run(query).getResponseCode();
    Assert.assertEquals("Checking command success", 0, ret);
  }

  @AfterClass
  public static void Teardown() throws Exception {
    Driver driver = createDriver();
    driver.run("drop table t1");
    driver.run("drop table t2");
    driver.run("drop view v1");
    driver.run("drop table car");
    driver.run("drop table dummy");
  }

  @Test
  public void testQueryTable1() throws ParseException {
    String query = "select * from t1";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));

    // check access columns from readEntitycdv
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
  }

  @Test
  public void testJoinTable1AndTable2() throws ParseException {
    String query = "select * from default.t1 join default.t2 on ((t1.id1 = t2.id1) AND (t1.name1 = t2.name2))";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
    cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("id2"));
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));


    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
    cols = tableColsMap.get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("id2"));
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
  }

  @Test
  public void testJoinView1AndTable2() throws ParseException {
    String query = "select * from v1 join t2 on (v1.id1 = t2.id1)";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    // t1 is inside v1, we should not care about its access info.
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t1");
    Assert.assertNull(cols);
    // v1 is top level view, we should care about its access info.
    cols = columnAccessInfo.getTableToColumnAccessMap().get("default@v1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
    cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("id2"));
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));


    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@t1");
    Assert.assertNull(cols);
    cols = tableColsMap.get("default@v1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
    cols = tableColsMap.get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("id2"));
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
  }

  private Map<String, List<String>> getColsFromReadEntity(HashSet<ReadEntity> inputs) {
    Map<String, List<String>> tableColsMap = new HashMap<String, List<String>>();
    for(ReadEntity entity: inputs) {
      switch (entity.getType()) {
        case TABLE:
          if (entity.getAccessedColumns() != null && !entity.getAccessedColumns().isEmpty()) {
            tableColsMap.put(entity.getTable().getCompleteName(), entity.getAccessedColumns());
          }
          break;
        case PARTITION:
          if (entity.getAccessedColumns() != null && !entity.getAccessedColumns().isEmpty()) {
            tableColsMap.put(entity.getPartition().getTable().getCompleteName(),
                    entity.getAccessedColumns());
          }
          break;
        default:
          // no-op
      }
    }
    return tableColsMap;
  }

  private static Driver createDriver() {
    HiveConf conf = new HiveConf(Driver.class);
    conf
            .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
                    "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS, true);
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    driver.init();
    return driver;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////
  @Test
  public void testUnnestWithOn() throws ParseException {
    String query = "select category.quantity, category.name, models.family,tradema.name,res.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks on (tradema.trademark_id = cars.trademark_id_ref) as tradema " +
            "unnest resellers on (cars.reseller_id_ref = res.reseller_id) res";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
  }

  @Test
  public void testUnnestWithWhereClause() throws ParseException {
    Driver driver = createDriver();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks as trademarks " +
            "unnest resellers resellers "+
            "where cars.trademark_id_ref = trademarks.trademark_id " +
            "and cars.reseller_id_ref = resellers.reseller_id";
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
  }

  @Test
  public void testUnnestWithONandWhereClause() throws ParseException {
    Driver driver = createDriver();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks on (trademarks.trademark_id = cars.trademark_id_ref) as trademarks " +
            "unnest resellers on ((cars.reseller_id_ref = resellers.reseller_id)) resellers " +
            "where trademarks.name = resellers.name and (resellers.name = 'toto')";
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
  }

  @Test
  public void testUnnestWithLateralView() throws ParseException {
    Driver driver = createDriver();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "lateral view inline(cars) cars " +
            "lateral view inline(cars.models) models " +
            "lateral view inline(models.category) category "+
            "unnest trademarks as trademarks " +
            "unnest resellers resellers "+
            "where cars.trademark_id_ref = trademarks.trademark_id " +
            "and cars.reseller_id_ref = resellers.reseller_id";
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
  }

  @Test
  public void testLateralView() throws ParseException {
    Driver driver = createDriver();
    String query = "select resellers.name from car " +
            "lateral view inline(cars) cars " +
            //"lateral view inline(cars.models) models " +
            //"lateral view inline(models.category) category "+
            "lateral view inline(trademarks) trademarks " +
            "lateral view inline(resellers) resellers";
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
  }

  @Test
  public void testUnnestWithOutConditions() throws ParseException {
    String query = "select res.name from car " +
            "unnest cars cars " +
            "unnest resellers res";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("cars"));
    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("cars"));
  }

  @Test
  public void testUnnestWithONConditions() throws ParseException {
    String query = "select res.name from car " +
            "unnest cars cars " +
            "unnest resellers on (cars.reseller_id_ref = res.reseller_id) res";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("cars"));
    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("cars"));
  }

  @Test
  public void testUnnestWithSelAll() throws ParseException {
    String query = "select * from car " +
            "unnest cars cars " +
            "unnest resellers on (cars.reseller_id_ref = res.reseller_id) res";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@car");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("cars"));
    Assert.assertNotNull(cols.contains("resellers"));
    Assert.assertNotNull(cols.contains("trademarks"));
  }



  @Test
  public void testUnnestWithOutConditions2() throws ParseException {
    String query = "select * from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category " +
            "unnest trademarks as trademarks " +
            "unnest resellers on (res.name = 'toto') res";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);

    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@car");
    Assert.assertNotNull(cols);
  }

  @Test
  public void testRunLateralView2() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.reseller_id_ref, res.name from car " +
            "lateral view inline(cars) cars " +
            //"lateral view inline(cars.models) models " +
            //"lateral view inline(models.category) category "+
            //"lateral view inline(trademarks) trademarks " +
            "lateral view inline(resellers) res "+
            "where res.name = 'GARAGE PARIS'";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(4, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnestWithOutConditions() throws ParseException, CommandNeedRetryException, IOException {
    String query = "select cars.reseller_id_ref, res.name from car " +
            "unnest cars cars " +
            "unnest resellers res";
    List<Object> result = new ArrayList<Object>();
    Driver driver = createDriver();
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(24, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnestWithONConditions() throws ParseException, CommandNeedRetryException, IOException {
    String query = "select cars.reseller_id_ref, res.name from car " +
            "unnest cars cars " +
            "unnest resellers on (cars.reseller_id_ref = res.reseller_id and res.reseller_id = cars.trademark_id_ref) res";
    List<Object> result = new ArrayList<Object>();
    Driver driver = createDriver();
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(2, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnestWithONConditions4() throws ParseException, CommandNeedRetryException, IOException {
    String query = "select cars.reseller_id_ref, res.name from car " +
            "unnest cars cars " +
            "unnest resellers on (res.name = 'GARAGE PARIS') res";
    //cars.reseller_id_ref = res.reseller_id and
    List<Object> result = new ArrayList<Object>();
    Driver driver = createDriver();
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(4, result.size());
    driver.close();
  }


  @Test
  public void testRunUnnest4() throws ParseException, CommandNeedRetryException, IOException {
    String query = "select res.name from car " +
            //"unnest cars cars " +
            "unnest resellers on (res.name = 'GARAGE PARIS') res "+
            "where res.name = 'GARAGE PARIS'";
    List<Object> result = new ArrayList<Object>();
    Driver driver = createDriver();
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(1, result.size());
    driver.close();
  }

  @Test
  public void testRunLateralView4() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select res.name from car " +
            //"lateral view inline(cars) cars " +
            //"lateral view inline(cars.models) models " +
            //"lateral view inline(models.category) category "+
            //"lateral view inline(trademarks) trademarks " +
            "lateral view outer inline(resellers) res "+
            "where res.name = 'GARAGE PARIS'";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(1, result.size());
    driver.close();
  }


  @Test
  public void testRunUnnest3() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.reseller_id_ref, res.name from car " +
            "unnest cars cars " +
            //"lateral view inline(cars.models) models " +
            //"lateral view inline(models.category) category "+
            //"lateral view inline(trademarks) trademarks " +
            "unnest resellers res "+
            "where cars.reseller_id_ref = 1";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(6, result.size());
    driver.close();
  }

  @Test
  public void testRunLateralView3() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.reseller_id_ref, res.name from car " +
            "lateral view inline(cars) cars " +
            //"lateral view inline(cars.models) models " +
            //"lateral view inline(models.category) category "+
            //"lateral view inline(trademarks) trademarks " +
            "lateral view inline(resellers) res "+
            "where cars.reseller_id_ref = 1";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(6, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest5a() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks as trademarks " +
            "unnest resellers resellers "+
            "where cars.trademark_id_ref = trademarks.trademark_id " +
            "and cars.reseller_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(22, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest5b() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,tradema.name,res.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks on (tradema.trademark_id = cars.trademark_id_ref) as tradema " +
            "unnest resellers on (cars.reseller_id_ref = res.reseller_id) res";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(22, result.size());
    driver.close();
  }

  @Test
  public void testRunLateralView5() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "lateral view inline(cars) cars " +
            "lateral view inline(cars.models) models " +
            "lateral view inline(models.category) category "+
            "unnest trademarks as trademarks " +
            "unnest resellers resellers "+
            "where cars.trademark_id_ref = trademarks.trademark_id " +
            "and cars.reseller_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(22, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest6a() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks as trademarks " +
            "unnest resellers on (resellers.name = 'GARAGE PARIS') resellers "+
            "where cars.trademark_id_ref = trademarks.trademark_id " +
            "and cars.reseller_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(4, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest6b() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,tradema.name,res.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks on (tradema.trademark_id = cars.trademark_id_ref) as tradema " +
            "unnest resellers on (cars.reseller_id_ref = res.reseller_id) res "+
            "where res.name = 'GARAGE PARIS'";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(4, result.size());
    driver.close();
  }

  @Test
  public void testRunLateralView6() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "lateral view inline(cars) cars " +
            "lateral view inline(cars.models) models " +
            "lateral view inline(models.category) category "+
            "unnest trademarks as trademarks " +
            "unnest resellers resellers "+
            "where cars.trademark_id_ref = trademarks.trademark_id " +
            "and cars.reseller_id_ref = resellers.reseller_id "+
            "and resellers.name = 'GARAGE PARIS'";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(4, result.size());
    driver.close();
  }


  @Test
  public void testRunLateralView7() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select sum(quantity),name,family,trademark,reseller from (\n" +
            "select category.quantity as quantity,\n" +
            "  category.name as name,\n" +
            "  models.family as family,\n" +
            "  trademarks.name as trademark,\n" +
            "  resellers.name as reseller \n" +
            "from car\n" +
            "lateral view inline(cars) cars\n" +
            "lateral view inline(cars.models) models\n" +
            "lateral view inline(models.category) category\n" +
            "lateral view  inline(trademarks) trademarks\n" +
            "lateral view inline(resellers) resellers\n" +
            "where cars.trademark_id_ref = trademarks.trademark_id\n" +
            "and cars.reseller_id_ref = resellers.reseller_id\n" +
            ") x\n" +
            "group by trademark,family,name,reseller";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(22, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest7a() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select sum(quantity),name,family,trademark,reseller from (\n" +
            "select category.quantity as quantity,\n" +
            "  category.name as name,\n" +
            "  models.family as family,\n" +
            "  trademarks.name as trademark,\n" +
            "  resellers.name as reseller \n" +
            "from car\n" +
            "unnest cars cars\n" +
            "unnest cars.models models\n" +
            "unnest models.category category\n" +
            "unnest trademarks on (cars.trademark_id_ref = trademarks.trademark_id) trademarks\n" +
            "unnest resellers on (cars.reseller_id_ref = resellers.reseller_id) resellers\n" +
            ") x\n" +
            "group by trademark,family,name,reseller";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(22, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest8() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "unnest cars cars " +
            "unnest cars.models models " +
            "unnest models.category category "+
            "unnest trademarks as trademarks " +
            "unnest resellers on (resellers.reseller_id = cars.reseller_id_ref) resellers " +
            "where cars.trademark_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(16, result.size());
    driver.close();
  }

  @Test
  public void testRunLV8() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select category.quantity, category.name, models.family,trademarks.name,resellers.name from car " +
            "lateral view inline(cars) cars " +
            "lateral view inline(cars.models) models " +
            "lateral view inline(models.category) category "+
            "lateral view inline(trademarks) trademarks " +
            "lateral view inline(resellers) resellers " +
            "where cars.trademark_id_ref = resellers.reseller_id and resellers.reseller_id = cars.reseller_id_ref";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(16, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest9() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select resellers.name from car " +
            "unnest cars cars " +
            //"unnest cars.models models " +
            //"unnest models.category category "+
            //"unnest trademarks as trademarks " +
            "unnest resellers on ( cars.trademark_id_ref = 1) resellers ";
    //"where cars.trademark_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(18, result.size());
    driver.close();
  }

  @Test
  public void testRunLV9() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select resellers.name from car " +
            "lateral view inline(cars) cars " +
            //"unnest cars.models models " +
            //"unnest models.category category "+
            //"unnest trademarks as trademarks " +
            "lateral view inline(resellers) resellers "+
            "where cars.trademark_id_ref = 1";
    //"where cars.trademark_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(18, result.size());
    driver.close();
  }


  @Test
  public void testRunUnnest10a() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.reseller_id_ref, resellers.* from car " +
            "unnest cars cars " +
            //"unnest cars.models models " +
            //"unnest models.category category "+
            //"unnest trademarks as trademarks " +
            "unnest resellers on ( cars.reseller_id_ref = resellers.reseller_id ) resellers ";
    //"where cars.trademark_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(8, result.size());
    driver.close();
  }

  @Test
  public void testRunUnnest10b() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select resellers.* from car " +
            "unnest cars cars " +
            //"unnest cars.models models " +
            //"unnest models.category category "+
            //"unnest trademarks as trademarks " +
            "unnest resellers resellers "+
            "where cars.trademark_id_ref > resellers.reseller_id";
    //"where cars.trademark_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(2, result.size());
    driver.close();
  }


  @Test
  public void testRunLV10() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.reseller_id_ref, resellers.* from car " +
            "lateral view inline(cars) cars " +
            //"unnest cars.models models " +
            //"unnest models.category category "+
            //"unnest trademarks as trademarks " +
            "lateral view outer inline(resellers) resellers "+
            "where cars.reseller_id_ref = resellers.reseller_id";
    //"where cars.trademark_id_ref = resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(8, result.size());
    driver.close();
  }


  @Test
  public void testJoinTable1AndTable22() throws ParseException {
    String query = "select * from default.t1 join default.t2 on (t1.id1 > t2.id1)";
    Driver driver = createDriver();
    int rc = driver.compile(query);
    Assert.assertEquals("Checking command success", 0, rc);
    QueryPlan plan = driver.getPlan();
    // check access columns from ColumnAccessInfo
    ColumnAccessInfo columnAccessInfo = plan.getColumnAccessInfo();
    List<String> cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
    cols = columnAccessInfo.getTableToColumnAccessMap().get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("id2"));
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));


    // check access columns from readEntity
    Map<String, List<String>> tableColsMap = getColsFromReadEntity(plan.getInputs());
    cols = tableColsMap.get("default@t1");
    Assert.assertNotNull(cols);
    Assert.assertEquals(2, cols.size());
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
    cols = tableColsMap.get("default@t2");
    Assert.assertNotNull(cols);
    Assert.assertEquals(3, cols.size());
    Assert.assertNotNull(cols.contains("id2"));
    Assert.assertNotNull(cols.contains("id1"));
    Assert.assertNotNull(cols.contains("name1"));
  }

  @Test
  public void testRunOuterUnnest1() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.reseller_id_ref, resellers.* from car " +
            "unnest cars cars " +
            "left outer unnest resellers on ( cars.reseller_id_ref = resellers.reseller_id ) resellers ";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(8, result.size());
    driver.close();
  }

  @Test
  public void testRunOuterUnnest2() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.reseller_id_ref, resellers.* from car " +
            "unnest cars cars " +
            "left outer unnest resellers on ( cars.reseller_id_ref = resellers.reseller_id and " +
            "cars.trademark_id_ref = resellers.reseller_id ) resellers ";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(8, result.size());
    driver.close();
  }

  @Test
  public void testRunOuterUnnestWithFilter() throws ParseException, CommandNeedRetryException, IOException {
    Driver driver = createDriver();
    List<Object> result = new ArrayList<Object>();
    String query = "select cars.trademark_id_ref, cars.reseller_id_ref, resellers.* from car " +
            "unnest cars cars " +
            "left outer unnest resellers on ( cars.reseller_id_ref = resellers.reseller_id ) resellers " +
            "where cars.trademark_id_ref < resellers.reseller_id";
    CommandProcessorResponse response = driver.run(query);
    Assert.assertEquals("Checking command success", 0, response.getResponseCode());
    driver.getResults(result);
    Assert.assertEquals(6, result.size());
    driver.close();
  }

































}



