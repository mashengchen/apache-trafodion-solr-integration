
/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.schema.AnalyzerDefinition;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest.Update;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CoreAdminParams;
import org.trafodion.udf.Settings;
import org.trafodion.udf.Utils;

public class SolrMain {

  private SolrClient getSolrClient() throws FileNotFoundException {

    String yamlFile = "remote-objects.yaml";
    File f = new File(yamlFile);
    if (!f.exists()) {
      throw new FileNotFoundException(f.getName());
    }
    Settings s = Settings.read(new FileInputStream(f));

    CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder().withZkHost(Utils.getZkHosts())
        .withZkChroot(Utils.getZkChroot()).build();
    cloudSolrClient.setDefaultCollection(Utils.getDefaultCollection());

    try {
      SolrPingResponse ping = cloudSolrClient.ping();
      ping.getStatus();
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }
    return cloudSolrClient;
  }

  @SuppressWarnings("deprecation")
  public void test(SolrClient solr) throws SolrServerException, IOException {
    SolrRequest request = new CollectionAdminRequest.List();

    List colls =
        (List) ((CollectionAdminResponse) request.process(solr)).getResponse().get("collections");

    System.out.println("all collections : " + colls);

    CoreAdminRequest coreRequest = new CoreAdminRequest();
    coreRequest.setAction(CoreAdminParams.CoreAdminAction.STATUS);
    CoreAdminResponse coreResponse = (CoreAdminResponse) coreRequest.process(solr);

    Set cores = new HashSet();
    for (Map.Entry entry : coreResponse.getCoreStatus()) {
      cores.add(entry.getKey());
    }
    if (!colls.contains("ttt")) {
      CollectionAdminRequest.Create collreq = new CollectionAdminRequest.Create();
      collreq.setNumShards(Integer.valueOf(1));
      collreq.setReplicationFactor(Integer.valueOf(1));
      collreq.setConfigName("basic_configs");
      collreq.setCollectionName("ttt");
      CollectionAdminResponse collresp = (CollectionAdminResponse) collreq.process(solr);
      System.out.println(collresp);
    }
  }

  public void addFieldType(SolrClient solr) {
    FieldTypeDefinition ftd = new FieldTypeDefinition();

    String name = "solr_cnAnalyzer";
    String clazz = "solr.TextField";
    String positionIncrementGap = "100";

    Map<String, Object> fieldAttributes = new LinkedHashMap<String, Object>();
    fieldAttributes.put("name", name);
    fieldAttributes.put("class", clazz);
    fieldAttributes.put("positionIncrementGap", positionIncrementGap);

    AnalyzerDefinition ad = new AnalyzerDefinition();
    Map<String, Object> tokenizer = new HashMap<String, Object>();
    tokenizer.put("class", "org.wltea.analyzer.solr.IKTokenizerFactory");
    ad.setTokenizer(tokenizer);

    ftd.setAttributes(fieldAttributes);
    ftd.setIndexAnalyzer(ad);
    ftd.setQueryAnalyzer(ad);

    SchemaRequest.FieldType ft = new SchemaRequest.FieldType("solr_cnAnalyzer");
    try {
      SchemaResponse.FieldTypeResponse ftresp = ft.process(solr);
      System.out.println(ftresp);
    } catch (SolrServerException | IOException e1) {
      e1.printStackTrace();
    }

    // SchemaRequest.AddFieldType adf = new SchemaRequest.AddFieldType(ftd);
    // try {
    // SchemaResponse.UpdateResponse resp = adf.process(solr);
    // System.out.println(resp);
    // } catch (SolrServerException | IOException e) {
    // e.printStackTrace();
    // }

  }

  public void addDynamicField(SolrClient solr) {
    // curl -X POST -H 'Content-type:application/json' --data-binary
    // '{"add-dynamic-field" : {
    // "name":"*_cn_s","type":"solr_cnAnalyzer","indexed":true,"stored":false}}'
    // http://localhost:8983/solr/test_collection/schema
    String name = "*_cn_s";
    String type = "solr_cnAnalyzer";

    Map<String, Object> fieldAttributes = new LinkedHashMap<String, Object>();
    fieldAttributes.put("name", name);
    fieldAttributes.put("type", type);
    fieldAttributes.put("indexed", Boolean.valueOf(true));
    fieldAttributes.put("stored", Boolean.valueOf(false));
    SchemaRequest.AddDynamicField adf = new SchemaRequest.AddDynamicField(fieldAttributes);
    try {
      SchemaResponse.UpdateResponse resp = adf.process(solr);
      System.out.println(resp);
    } catch (SolrServerException | IOException e) {
      e.printStackTrace();
    }

  }

  public void schemaUpdate(SolrClient solr) {
    String[] columns = { "D", "D", "A", "B" };
    List<Update> list = new ArrayList<Update>();

    Map<String, Object> fieldAttributes = new LinkedHashMap<String, Object>();
    String contentColumn = columns[0];
    fieldAttributes.put("type", "string");
    fieldAttributes.put("indexed", Boolean.valueOf(true));

    fieldAttributes.put("name", contentColumn);
    fieldAttributes.put("stored", Boolean.valueOf(false));
    SchemaRequest.AddField contentField = new SchemaRequest.AddField(fieldAttributes);
    list.add(contentField);

    for (int i = 1; i < columns.length; i++) {
      String colName = columns[i];
      if (!contentColumn.equalsIgnoreCase(colName)) {
        fieldAttributes.put("name", colName);
        fieldAttributes.put("stored", Boolean.valueOf(true));
        SchemaRequest.AddField field = new SchemaRequest.AddField(fieldAttributes);
        list.add(field);
      }
    }
    SchemaRequest.MultiUpdate mu = new SchemaRequest.MultiUpdate(list);
    List<Map> errorList = null;
    try {
      SchemaResponse.UpdateResponse addFieldResponse =
          (SchemaResponse.UpdateResponse) mu.process(solr);
      errorList = (List<Map>) addFieldResponse.getResponse().get("errors");
    } catch (SolrServerException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (errorList != null) {
      list.clear();
      for (Map map : errorList) {
        if ("schema is not editable".equals(map.get("errorMessages"))) {
          return;
        }
        String errMsg = ((String) ((List) map.get("errorMessages")).get(0)).toLowerCase();
        if ((errMsg != null) && (errMsg.indexOf("field") != -1)
            && (errMsg.indexOf("already exists") != -1)) {
          String name = (String) ((Map) map.get("add-field")).get("name");
          fieldAttributes.put("name", name);
          fieldAttributes.put("type", "string");
          fieldAttributes.put("indexed", Boolean.valueOf(true));

          if (contentColumn.equals(name)) fieldAttributes.put("stored", Boolean.valueOf(false));
          else {
            fieldAttributes.put("stored", Boolean.valueOf(true));
          }
          SchemaRequest.ReplaceField replaceField = new SchemaRequest.ReplaceField(fieldAttributes);
          list.add(replaceField);
        }
      }

      mu = new SchemaRequest.MultiUpdate(list);
      try {
        mu.process(solr);
      } catch (SolrServerException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void addIndex(SolrClient solr) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1003");
    doc.addField("name_s", "test3");// _s String类型
    doc.addField("score_i", "103");// _i 整型
    solr.add(doc);
    solr.commit();
    // 添加多个
    Collection<SolrInputDocument> docs = new ArrayList();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField("id", i);
      doc2.addField("name_s", "test3");// _s String类型
      doc2.addField("score_i", i);// _i 整型
      docs.add(doc2);
    }
    solr.add(docs);
    solr.commit();
  }

  public static void query(SolrClient solr) throws Exception {
    SolrQuery query = new SolrQuery();
    // query.setQuery("*:*");
    // query.set("q", "*:*");
    // query.set("q", "name_s:test1?");//?通配单个字符
    // query.set("q", "name_s:tes*");//*通配多个字符
    // query.set("q", "name_s:test~0.5");// ~ 模糊查询 可以直接用test~
    // test~0.5表示相似0.5以上的
    // 如检索相隔10个单词的“apache”和”“akarta”，“jakarta apache”~10
    query.set("q", "name_s:test15 or name_s:test16");

    /*
     * 布尔操作符AND、|| 布尔操作符OR、&& 布尔操作符NOT、!、-（排除操作符不能单独与项使用构成查询） “+” 存在操作符，要求符号“+”后的项必须在文档相应的域中存在 ( )
     * 用于构成子查询 [ ] 包含范围检索，如检索某时间段记录，包含头尾，date:[200707 TO 200710] { }不包含范围检索，如检索某时间段记录，不包含头尾
     * date:{200707 TO 200710} " 转义操作符，特殊字符包括+ - && || ! ( ) { } [ ] ^ ” ~ * ? : "
     */

    /*
     * 查询某个字段非空的记录 比如：fq=FieldName:[‘’ TO *] 查询FieldName非空的数据。 查询某个字段为空的记录
     * 比如：查询公司名称为空的记录可以采用如下语法实现(似乎目前为止只有此方法可行): -company:[* TO *] 取反实例：fq=!fstate:1
     */

    // “^”控制相关度检索，如检索jakarta
    // apache，同时希望去让“jakarta”的相关度更加好，那么在其后加上”^”符号和增量值，即jakarta^4 apache
    query.set("fl", "score_i,name_s");// fl 查询字段
    query.set("sort", "score_i desc");// sort 排序方式,正序用asc
    // wt 输出格式：json xml等

    query.set("fq", "score_i:[15 TO *]");// 分数>=15 fq
                                         // 过滤条件：在q查询符合结果中同时是fq查询符合的
    query.setStart(0);// 开始记录数
    query.setRows(10000);// 总条数
    QueryResponse queryResponse = solr.query(query);
    List<TestBean> results = queryResponse.getBeans(TestBean.class);
    System.out.println("总条数为：" + results.size());
    for (TestBean testBean : results) {
      System.out.println(testBean.toString());
    }
  }

  public static void query1(SolrClient solr) throws Exception {
    SolrQuery query = new SolrQuery();
    /*
     * 1、常用 q :查询字符串，这个是必须的。如果查询所有*:* ，根据指定字段查询（Name:张三 AND Address:北京） 注意：AND要大写 否则会被当做默认OR
     */
    // query.setQuery("*:*");

    /*
     * fq : （filter query）过虑查询，作用：在q查询符合结果中同时是fq查询符合的， 例如：q=查询全部&fq=只要title得值为:xxxx
     */
    // query.setFilterQueries("typeName:测试");

    /*
     * fl : 指定返回那些字段内容，用逗号或空格分隔多个。
     */
    // query.setFields("typeId,typeName");

    /*
     * sort : 排序，格式：sort=<field name>+<desc|asc>[,<field name>+<desc|asc>]… 。 示例：（score desc, price
     * asc）表示先 “score” 降序, 再 “price” 升序，默认是相关性降序。
     */
    SortClause sort1 = new SortClause("createTime", ORDER.asc);
    SortClause sort2 = new SortClause("updateTime", ORDER.desc);
    List<SortClause> sortList = new ArrayList<SortClause>();
    sortList.add(sort1);
    sortList.add(sort2);
    // query.setSort("createTime", ORDER.asc);
    query.setSorts(sortList);

    query.setQuery("typeName:测试"); // 设置查询关键字
    query.setHighlight(true); // 开启高亮
    query.addHighlightField("typeName"); // 高亮字段
    query.setHighlightSimplePre("<font color='red'>"); // 高亮单词的前缀
    query.setHighlightSimplePost("</font>"); // 高亮单词的后缀

    query.setStart(0);// 开始记录数
    query.setRows(10000);// 总条数

    // QueryResponse queryResponse = server.query(query);
    // List<GoodsType> results = queryResponse.getBeans(GoodsType.class);
    // System.out.println("总条数为：" + results.size());
    // for (GoodsType testBean : results) {
    // System.out.println(testBean.toString());
    // }

    QueryResponse queryResponse = solr.query(query);
    List<TestBean> results = queryResponse.getBeans(TestBean.class);
    int index = 0;
    // 返回所有的结果...
    SolrDocumentList documentList = queryResponse.getResults();
    Map<String, Map<String, List<String>>> maplist = queryResponse.getHighlighting();
    System.out.println(documentList);
    System.out.println(maplist);

    for (SolrDocument document : documentList) {
      String typeId = (String) document.getFieldValue("typeId");
      String typeName = maplist.get(typeId).get("typeName").get(0);
      results.get(index).setTypeName(typeName);
      index++;
    }

    for (TestBean type : results) {
      System.out.println(type);
    }

  }

  public static void multicoreQuery(SolrClient solr, String queryStr) throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery(queryStr);
    // query.setStart(0);//开始记录数
    // query.setRows(30);//总条数
    query.set("start", 0);
    query.set("rows", 20);
    query.set("shards",
      "localhost:8081/solr/test" + ",localhost:8081/solr/core1" + ",localhost:8081/solr/core2");
    // shards可以关联多个core，用逗号分隔. id 是唯一主键，多个核的id要不一样
    QueryResponse queryResponse = solr.query(query);
    List<TestBean> results = queryResponse.getBeans(TestBean.class);
    System.out.println("总条数为：" + results.size());
    for (TestBean testBean : results) {
      System.out.println(testBean.toString());
    }

  }

  public void createDoc(SolrClient solr, int id, String text)
      throws SolrServerException, IOException {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("subject", "TT");
    doc.addField("id", UUID.randomUUID().toString());
    doc.addField("traf_i", id);
    doc.addField("traf_s", "A");
    doc.addField("cnText", text);
    solr.add(doc);
    solr.commit();

  }

  public void query2(SolrClient solr, String key) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("NAME_cn_s:" + key);
    query.setFilterQueries("subject:TT");
    query.setFields("ID_i", "TYPE_s");

    System.out.println(query);
    QueryResponse response = solr.query(query);
    SolrDocumentList solrDocumentList = response.getResults();
    for (SolrDocument doc : solrDocumentList) {
      System.out.println(doc.get("ID_i") + " " + doc.get("TYPE_s"));
    }
    System.out.println();
  }

  class TestBean {

    public void setTypeName(String typeName) {
      // TODO Auto-generated method stub

    }
  }

  public void deleteByQuery(SolrClient solr) throws Exception {
    String query = "*:*";
    solr.deleteByQuery(query);
    solr.commit();
  }

  public static void main(String[] args) throws Exception {
    String text = "支持中文，我觉得好帅气";
    String text1 = "支持中国，支持国货。我爱祖国";
    SolrMain main = new SolrMain();
    SolrClient client = main.getSolrClient();
    // main.deleteByQuery(client);

    // main.createDoc(client, 2, text1);
    // main.addFieldType(client);

    client.close();
    // getTblPKInfo();
  }

  private static void getTblPKInfo() {
    try {
      String trafHome = Utils.getTrafHome();
      String javaHome = Utils.getJavaHome();
      String classPath = Utils.getClassPath();
      String source = "cd " + trafHome + ";. sqenv.sh;";
      String export = "export LD_PRELOAD=" + javaHome + "/jre/lib/amd64/libjsig.so:" + trafHome
          + "/export/lib64/libseabasesig.so;";
      String runjar = trafHome + File.separator + "export" + File.separator + "lib" + File.separator
          + "SolrUDF-1.0.jar";
      String catalog = "TRAFODION";
      String schema = "SEABASE";
      String tblName = "TT";
      String java = "java -cp " + classPath + ":" + runjar + " org.trafodion.udf.DBUtils " + catalog
          + " " + schema + " " + tblName;

      Runtime rt = Runtime.getRuntime();
      Process proc = rt.exec(new String[] { "/bin/bash", "-c", source + export + java });

      BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
      String line = null;
      boolean hasError = false;
      while ((line = in.readLine()) != null) {
        if ("finish".equals(line)) {
          break;
        }
        if ("error".equals(line)) {
          hasError = true;
          break;
        }
        System.out.println(line);
      }
      if (hasError) {
        in = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        line = null;
        while ((line = in.readLine()) != null) {
          if ("finish".equals(line)) {
            break;
          }
          System.out.println(line);
        }
      }

      System.out.println("finish");

      proc.waitFor();
      System.out.println("finish all");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}