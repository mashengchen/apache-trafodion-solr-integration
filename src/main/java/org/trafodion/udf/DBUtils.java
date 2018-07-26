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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public class DBUtils {
  static String driverClassName_ = "org.apache.trafodion.jdbc.t2.T2Driver";
  static String connectionString_ = "jdbc:t2jdbc:";
  static Connection conn_;

  static {
    try {
      Class.forName(driverClassName_);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static Connection getConnection(Properties prop) throws SQLException {
    return conn_ = DriverManager.getConnection(connectionString_, prop);
  }

  public static void disconnect() throws SQLException {
    if (conn_ != null) {
      conn_.close();
    }
    conn_ = null;
  }

  public static void getTableInfo(String catalog, String schema, String tblName) {

    Map<String, Object[]> colNameType = new HashMap<String, Object[]>();
    try {
      Properties prop = new Properties();
      prop.put("catalog", catalog);
      prop.put("schema", schema);
      Connection conn = DBUtils.getConnection(prop);

      DatabaseMetaData dbMeta = conn.getMetaData();
      ResultSet pkRSet = dbMeta.getPrimaryKeys(catalog, schema, tblName);
      while (pkRSet.next()) {
        colNameType.put(pkRSet.getObject(4).toString().toUpperCase(), null);
      }

      ResultSet colRet = dbMeta.getColumns(catalog, schema, tblName, "%");
      int colNum = 1;
      while (colRet.next()) {
        String colName = colRet.getString("COLUMN_NAME").toUpperCase();
        if (colNameType.containsKey(colName)) {
          int datasize = colRet.getInt("COLUMN_SIZE");
          int scale = colRet.getInt("DECIMAL_DIGITS");
          int nullable = colRet.getInt("NULLABLE");
          int datatype = colRet.getInt("DATA_TYPE");
          boolean signed = colRet.getMetaData().isSigned(colNum);
          Object[] objs = { colName, datatype, datasize, scale, nullable, signed };
          colNameType.put(colName, objs);
        }
        colNum++;
      }
      for (Entry<String, Object[]> entry : colNameType.entrySet()) {
        String value = Arrays.toString(entry.getValue());
        value = value.substring(1, value.length() - 1).replaceAll(" ", "");
        System.out.println(value);
      }
      System.out.println("finish");
    } catch (Exception e) {
      System.out.println("error");
      System.out.println(e.getMessage());
      System.out.println("finish");
    }
  }

  public static void main(String[] args) {
    DBUtils.getTableInfo(args[0], args[1], args[2]);
    try {
      DBUtils.disconnect();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
