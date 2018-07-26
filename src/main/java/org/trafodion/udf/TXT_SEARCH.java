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
package org.trafodion.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.trafodion.sql.udr.ColumnInfo;
import org.trafodion.sql.udr.TypeInfo;
import org.trafodion.sql.udr.UDR;
import org.trafodion.sql.udr.UDRException;
import org.trafodion.sql.udr.UDRInvocationInfo;
import org.trafodion.sql.udr.UDRPlanInfo;

/**
 *
 * @author shengchen.ma
 *
 *         <pre>
 *  create table_mapping function TXT_SEARCH(tblName varchar(100) not null, cols varchar(100), keyword varchar(1000) CHARACTER SET ucs2)
 *      external name 'org.trafodion.udf.TXT_SEARCH' -- name of your class
 *      language java
 *      library SolrUDFlib;
 * 
 *  SELECT * FROM UDF(TXT_SEARCH('tt','name', '中文'));
 * </pre>
 */
public class TXT_SEARCH extends UDR {
  private final static String[] typeName = { "_s", "_i", "_l", "_d" };
  private final static String CN = "_cn_s";
  private final static String EN = "_en_s";

  private SolrClient getSolrClient() throws UDRException {
    String homeDir = Utils.getTrafHome();

    CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder().withZkHost(Utils.getZkHosts())
        .withZkChroot(Utils.getZkChroot()).build();
    cloudSolrClient.setDefaultCollection(Utils.getDefaultCollection());

    try {
      SolrPingResponse ping = cloudSolrClient.ping();
      ping.getStatus();
    } catch (SolrServerException | IOException e) {
      throw new UDRException(38000,
          "error while connecting to full text search engine...please make sure configuration in file [%s] is correct...[%s]",
          homeDir + "/udr/public/conf/solr.yaml", e.getMessage());
    }
    return cloudSolrClient;
  }

  private TypeInfo getUDRTypeFromJDBCType(String[] colInfo) throws UDRException {
    // colInfo = {colName, datatype, datasize, scale, nullable, signed};
    TypeInfo result;

    final int maxLength = 100000;

    TypeInfo.SQLTypeCode sqlType = TypeInfo.SQLTypeCode.UNDEFINED_SQL_TYPE;

    int colJDBCType = Integer.parseInt(colInfo[1]);
    int length = Integer.parseInt(colInfo[2]);
    int scale = Integer.parseInt(colInfo[3]);
    boolean nullable = Integer.parseInt(colInfo[4]) == 0 ? false : true;
    boolean signed = Boolean.parseBoolean(colInfo[5]);

    TypeInfo.SQLCharsetCode charset = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;
    TypeInfo.SQLIntervalCode intervalCode = TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE;
    int precision = 0;
    TypeInfo.SQLCollationCode collation = TypeInfo.SQLCollationCode.SYSTEM_COLLATION;

    // map the JDBC type to a Trafodion UDR parameter type
    switch (colJDBCType) {
    case java.sql.Types.SMALLINT:
    case java.sql.Types.TINYINT:
    case java.sql.Types.BOOLEAN:
      if (signed) sqlType = TypeInfo.SQLTypeCode.SMALLINT;
      else sqlType = TypeInfo.SQLTypeCode.SMALLINT_UNSIGNED;
      break;

    case java.sql.Types.INTEGER:
      if (signed) sqlType = TypeInfo.SQLTypeCode.INT;
      else sqlType = TypeInfo.SQLTypeCode.INT_UNSIGNED;
      break;

    case java.sql.Types.BIGINT:
      sqlType = TypeInfo.SQLTypeCode.LARGEINT;
      break;

    case java.sql.Types.DECIMAL:
    case java.sql.Types.NUMERIC:
      if (signed) sqlType = TypeInfo.SQLTypeCode.NUMERIC;
      else sqlType = TypeInfo.SQLTypeCode.NUMERIC_UNSIGNED;
      precision = length;
      break;

    case java.sql.Types.REAL:
      sqlType = TypeInfo.SQLTypeCode.REAL;
      break;

    case java.sql.Types.DOUBLE:
    case java.sql.Types.FLOAT:
      sqlType = TypeInfo.SQLTypeCode.DOUBLE_PRECISION;
      break;

    case java.sql.Types.CHAR:
    case java.sql.Types.NCHAR:
      sqlType = TypeInfo.SQLTypeCode.CHAR;
      length = Math.min(length, maxLength);
      charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
      break;

    case java.sql.Types.VARCHAR:
    case java.sql.Types.NVARCHAR:
      sqlType = TypeInfo.SQLTypeCode.VARCHAR;
      length = Math.min(length, maxLength);
      charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
      break;

    case java.sql.Types.DATE:
      sqlType = TypeInfo.SQLTypeCode.DATE;
      break;

    case java.sql.Types.TIME:
      sqlType = TypeInfo.SQLTypeCode.TIME;
      break;

    case java.sql.Types.TIMESTAMP:
      sqlType = TypeInfo.SQLTypeCode.TIMESTAMP;
      scale = 3;
      break;

    // BLOB - not supported yet, map to varchar
    // case java.sql.Types.BLOB:
    // sqlType = TypeInfo.SQLTypeCode.BLOB;
    // break;

    // CLOB - not supported yet, map to varchar
    // case java.sql.Types.CLOB:
    // sqlType = TypeInfo.SQLTypeCode.CLOB;
    // break;

    case java.sql.Types.ARRAY:
    case java.sql.Types.BINARY:
    case java.sql.Types.BIT:
    case java.sql.Types.BLOB:
    case java.sql.Types.DATALINK:
    case java.sql.Types.DISTINCT:
    case java.sql.Types.JAVA_OBJECT:
    case java.sql.Types.LONGVARBINARY:
    case java.sql.Types.NULL:
    case java.sql.Types.OTHER:
    case java.sql.Types.REF:
    case java.sql.Types.STRUCT:
    case java.sql.Types.VARBINARY:
      // these types produce a binary result, represented
      // as varchar(n) character set iso88591
      sqlType = TypeInfo.SQLTypeCode.VARCHAR;
      length = Math.min(length, maxLength);
      charset = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;
      break;

    case java.sql.Types.LONGVARCHAR:
    case java.sql.Types.LONGNVARCHAR:
    case java.sql.Types.CLOB:
    case java.sql.Types.NCLOB:
    case java.sql.Types.ROWID:
    case java.sql.Types.SQLXML:
      // these types produce a varchar(n) character set utf8 result
      sqlType = TypeInfo.SQLTypeCode.VARCHAR;
      length = Math.min(length, maxLength);
      charset = TypeInfo.SQLCharsetCode.CHARSET_UCS2;
      break;
    }

    result =
        new TypeInfo(sqlType, length, nullable, scale, charset, intervalCode, precision, collation);

    return result;
  }

  // return colName & fieldType
  // eg: a string column [A] will be [A_s]
  private Map<String, String> genColFieldMap(UDRInvocationInfo info) throws UDRException {
    Map<String, String> map = new HashMap<String, String>();
    int numCols = info.out().getNumColumns();// get from out column
    for (int i = 0; i < numCols; i++) {
      ColumnInfo ci = info.out().getColumn(i);
      TypeInfo type = ci.getType();
      String fieldName = typeName[0];

      switch (type.getSQLType()) {
      case UNDEFINED_SQL_TYPE:
      case REAL:
      case DOUBLE_PRECISION:
      case CHAR:
      case VARCHAR:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case INTERVAL:
      case BLOB:
      case CLOB:
        // string
        fieldName = typeName[0];
        break;
      case SMALLINT:
      case INT:
      case SMALLINT_UNSIGNED:
      case INT_UNSIGNED:
        // int
        fieldName = typeName[1];
        break;
      case LARGEINT:
        // long
        fieldName = typeName[2];
        break;
      case NUMERIC:
      case DECIMAL_LSE:
      case NUMERIC_UNSIGNED:
      case DECIMAL_UNSIGNED:
        // double
        fieldName = typeName[3];
        break;
      default:
        // string
        fieldName = typeName[0];
        break;
      }
      map.put(ci.getColName(), ci.getColName() + fieldName);
    }
    return map;
  }

  @Override
  public void describeParamsAndColumns(UDRInvocationInfo info) throws UDRException {
    SolrClient solr = getSolrClient();
    try {
      solr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    // get pk by tbl name through t2's prepareStatement
    String tblName = info.par().getString(0).toUpperCase();
    Map<String, String[]> colNameType =
        Utils.getTablePKinfo(Utils.getCatalog(), Utils.getSchema(), tblName);
    if (colNameType.get("error") != null) {
      throw new UDRException(38000, "error while get table primart key info , error info [ %s ]",
          colNameType.get("error")[0]);
    }
    // set out columns
    for (Entry<String, String[]> entry : colNameType.entrySet()) {
      String key = entry.getKey();
      TypeInfo typeInfo = getUDRTypeFromJDBCType(entry.getValue());
      info.out().addColumn(new ColumnInfo(key, typeInfo));
      // info.out().addVarCharColumn(key, 100, false);
    }
    // info.addPassThruColumns();
  }

  @Override
  public void processData(UDRInvocationInfo info, UDRPlanInfo plan) throws UDRException {
    String tblName = info.par().getString(0).toUpperCase();
    String columnName = info.par().getString(1).toUpperCase();
    String keyWork = info.par().getString(2);

    Map<String, String> map = genColFieldMap(info);
    int outNumCols = info.out().getNumColumns();
    List<String> outCols = new ArrayList<String>();
    for (int i = 0; i < outNumCols; i++) {
      outCols.add(info.out().getColumn(i).getColName());
    }
    SolrClient solr = getSolrClient();
    SolrQuery query = new SolrQuery();
    String queryStr = null;
    if (Utils.isChinese(keyWork)) {
      queryStr = columnName + CN + ":" + keyWork;
    } else {
      queryStr = columnName + EN + ":" + keyWork;
    }
    query.setQuery(queryStr);
    query.setFilterQueries("subject:" + tblName);
    query.setFields(map.values().toArray(new String[map.values().size()]));
    try {
      QueryResponse response = solr.query(query);
      SolrDocumentList solrDocumentList = response.getResults();
      for (SolrDocument doc : solrDocumentList) {
        for (int i = 0; i < outCols.size(); i++) {
          String fieldName = map.get(outCols.get(i));
          info.out().setString(i, doc.get(fieldName).toString());
        }
        emitRow(info);
      }
      solr.close();
    } catch (SolrServerException | IOException e) {
      try {
        solr.close();
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      e.printStackTrace();
    }
  }
}
