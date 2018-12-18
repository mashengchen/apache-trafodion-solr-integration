package org.trafodion.sql.udr.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Utils {
  private static long timeStamp;
  private static String yamlFile;

  private static String defaultCollection = null;
  private static String schema = null;
  private static String catalog = null;
  private static List<String> zkhosts = null;
  private static String zkChroot = null;
  public static int batch = 100;

  private static String trafHome;
  private static String javaHome;
  private static String classPath;

  static {
    try {
      init();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static List<String> getZkHosts() {
    if (isConfFileUpdated()) {
      reload();
    }
    return zkhosts;
  }

  public static String getZkChroot() {
    if (isConfFileUpdated()) {
      reload();
    }
    return zkChroot;
  }

  public static String getDefaultCollection() {
    if (isConfFileUpdated()) {
      reload();
    }
    return defaultCollection;
  }

  public static String getSchema() {
    if (isConfFileUpdated()) {
      reload();
    }
    return schema;
  }

  public static String getCatalog() {
    if (isConfFileUpdated()) {
      reload();
    }
    return catalog;
  }

  private static boolean isConfFileUpdated() {
    File file = new File(yamlFile);
    long ts = file.lastModified();
    if (timeStamp != ts) {
      timeStamp = ts;
      // Yes, file is updated
      return true;
    }
    // No, file is not updated
    return false;
  }

  private static void reload() {
    try {
      Settings s = Settings.read(new FileInputStream(new File(yamlFile)));
      zkhosts = s.getZkhosts();
      zkChroot = s.getZkChroot();
      defaultCollection = s.getDefaultCollection();
      schema = s.getSchema();
      catalog = s.getCatalog();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }

  }

  private static String genSolrFileContent() {
    // defaultCollection : traf_collection
    // schema : SEABASE
    // catalog : TRAFODION
    // zkhosts : [192.168.0.11:2181, 192.168.0.12:2181]
    StringBuffer sb = new StringBuffer();
    // generally Solr is on zoo nodes.
    String zkhostsStr = System.getenv("ZOO_PORT_NODES");

    if (zkhostsStr == null || zkhostsStr.length() == 0) {
      zkhostsStr = "localhost:2181";
    }
    zkhosts = Arrays.asList(zkhostsStr.replaceAll(" ", "").split(","));
    zkChroot = "/solr";
    defaultCollection = "traf_collection";
    catalog = "TRAFODION";
    schema = "SEABASE";
    sb.append("zkhosts:").append(System.getProperty("line.separator"));
    for (String zkhost : zkhosts) {
      sb.append(" - ").append(zkhost).append(System.getProperty("line.separator"));
    }
    sb.append("zkChroot: ").append(zkChroot).append(System.getProperty("line.separator"));
    sb.append("defaultCollection: ").append(defaultCollection)
        .append(System.getProperty("line.separator"));
    sb.append("catalog: ").append(catalog).append(System.getProperty("line.separator"));
    sb.append("schema: ").append(schema);

    return sb.toString();
  }

  public static String getTrafHome() {
    return trafHome;
  }

  public static String getJavaHome() {
    return javaHome;
  }

  public static String getClassPath() {
    return classPath;
  }

  private static synchronized void init() throws IOException {
    trafHome = System.getenv().get("MY_SQROOT");// for version before 2.3
    if (trafHome == null || trafHome.length() == 0) {
      trafHome = System.getenv("TRAF_HOME");// for version after 2.3
    }
    javaHome = System.getenv().get("JAVA_HOME");// maybe need version check in the future
    classPath = System.getenv().get("CLASSPATH");// maybe need do some filter in the future

    String homeDir = Utils.getTrafHome();
    String fileDir = homeDir + "/udr/public/conf";
    File path = new File(fileDir);
    if (!path.exists()) {
      path.mkdirs();
    }
    yamlFile = homeDir + "/udr/public/conf/solr.yaml";
    File yaml = new File(yamlFile);
    if (!yaml.exists()) {
      yaml.createNewFile();
      FileOutputStream out = new FileOutputStream(yaml);
      out.write(genSolrFileContent().getBytes());
      out.flush();
      out.close();
    } else {
      reload();
    }
    timeStamp = yaml.lastModified();
  }

  public static Map<String, String[]> getTablePKinfo(String catalog, String schema,
      String tblName) {
    Map<String, String[]> map = new HashMap<String, String[]>();
    try {
      String trafHome = Utils.getTrafHome();
      String javaHome = Utils.getJavaHome();
      String classPath = Utils.getClassPath();
      String source = "cd " + trafHome + ";. sqenv.sh;";
      String export = "export LD_PRELOAD=" + javaHome + "/jre/lib/amd64/libjsig.so:" + trafHome
          + "/export/lib64/libseabasesig.so;";
      String runjar = trafHome + File.separator + "export" + File.separator + "lib" + File.separator
          + "SolrUDF-1.0.jar";
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
        String[] arr = line.split(",");
        map.put(arr[0], arr);
      }
      if (hasError) {
        // in = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        line = null;
        StringBuffer sb = new StringBuffer();
        while ((line = in.readLine()) != null) {
          if ("finish".equals(line)) {
            break;
          }
          sb.append(line).append(System.getProperty("line.separator"));
        }
        map.put("error", new String[] { sb.toString() });
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return map;
  }

  public static void main(String[] args) {
    getTablePKinfo(Utils.getCatalog(), Utils.getSchema(), "TT");
  }

  private static boolean isChinese(char c) {
    Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
    if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
        || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
        || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
        || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
        || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
        || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
        || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
      return true;
    }
    return false;
  }

  public static boolean isChinese(String strName) {
    char[] ch = strName.toCharArray();
    for (int i = 0; i < ch.length; i++) {
      char c = ch[i];
      if (isChinese(c)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isChineseByREG(String str) {
    if (str == null) {
      return false;
    }
    Pattern pattern = Pattern.compile("[\\u4E00-\\u9FBF]+");
    return pattern.matcher(str.trim()).find();
  }

}
