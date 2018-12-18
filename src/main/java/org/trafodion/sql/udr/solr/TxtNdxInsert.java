package org.trafodion.sql.udr.solr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.common.SolrInputDocument;
import org.trafodion.sql.udr.ColumnInfo;
import org.trafodion.sql.udr.TableInfo;
import org.trafodion.sql.udr.TypeInfo;
import org.trafodion.sql.udr.UDR;
import org.trafodion.sql.udr.UDRException;
import org.trafodion.sql.udr.UDRInvocationInfo;
import org.trafodion.sql.udr.UDRPlanInfo;

/**
 *
 * @author shengchen.ma
 *
 *
 *         <pre>
 *  create table_mapping function txt_ndx_insert(tblName varchar(100) not null, cols varchar(100))
 *      external name 'org.trafodion.udf.txt_ndx_insert' -- name of your class
 *      language java
 *      library SolrUDFlib;
 *
 *
 *  select * FROM UDF (txt_ndx_insert (
 *      TABLE (SELECT name, id ,type FROM (INSERT INTO tt (id,type,name) VALUES (11, 'F', '支持中文'),(10, 'A', '我厂威武')) tt ),
 *      'tt',
 *      'name, id ,type'));
 * </pre>
 */

public class TxtNdxInsert extends UDR {
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

  @Override
  public void describeParamsAndColumns(UDRInvocationInfo info) throws UDRException {
    SolrClient solr = getSolrClient();
    try {
      solr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    info.out().addLongColumn("ROWS_INSERTED", false);
  }

  @Override
  public void processData(UDRInvocationInfo info, UDRPlanInfo plan) throws UDRException {

    String tblName = info.par().getString(0).toUpperCase(); // first param
                                                            // is tableName
    // second param is cols, first index is text type, follow is index
    String[] cols = info.par().getString(1).replaceAll(" ", "").toUpperCase().split(",");
    Map<String, String[]> pkMap =
        Utils.getTablePKinfo(Utils.getCatalog(), Utils.getSchema(), tblName);
    if (pkMap.get("error") != null) {
      throw new UDRException(38000, "error while get table primart key info , error info [ %s ]",
          pkMap.get("error")[0]);
    }
    Map<String, String> map = genColFieldMap(info);
    SolrClient solr = getSolrClient();
    SolrInputDocument doc = null;

    int count = 0;
    int numRowsInserted = 0;

    while (getNextRow(info)) {
      numRowsInserted++;
      count++;
      doc = new SolrInputDocument();
      doc.addField("id", UUID.randomUUID().toString());
      // full text content
      String textFieldName = null;
      if (Utils.isChinese(info.in().getString(cols[0]))) {
        textFieldName = cols[0] + CN;
      } else {
        textFieldName = cols[0] + EN;
      }
      doc.addField(textFieldName, info.in().getString(cols[0]));
      // filter
      doc.addField("subject", tblName);
      // traf table pk
      for (String pk : pkMap.keySet()) {
        String fieldName = map.get(pk);
        Object value = getValue(info, pk);
        doc.setField(fieldName, value);
      }
      try {
        solr.add(doc);
        if (count % Utils.batch == 0) {
          count -= Utils.batch;
          solr.commit();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    if (count > 0) {
      try {
        solr.commit();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    try {
      solr.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    info.out().setLong(0, numRowsInserted);
    emitRow(info);
  }

  // return colName & fieldType
  // eg: a string column [A] will be [A_s]
  private Map<String, String> genColFieldMap(UDRInvocationInfo info) throws UDRException {
    Map<String, String> map = new HashMap<String, String>();
    int numCols = info.in().getNumColumns();// get from input tbl
    for (int i = 1; i < numCols; i++) {
      ColumnInfo ci = info.in().getColumn(i);
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

  private Object getValue(UDRInvocationInfo info, String colName) throws UDRException {
    TableInfo ti = info.in();
    TypeInfo type = ti.getColumn(colName).getType();

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
      return ti.getString(colName);
    case SMALLINT:
    case INT:
    case SMALLINT_UNSIGNED:
    case INT_UNSIGNED:
      // int
      return ti.getInt(colName);
    case LARGEINT:
      // long
      return ti.getLong(colName);
    case NUMERIC:
    case DECIMAL_LSE:
    case NUMERIC_UNSIGNED:
    case DECIMAL_UNSIGNED:
      // double
      return ti.getDouble(colName);
    default:
      // string
      return ti.getString(colName);
    }
  }
}
