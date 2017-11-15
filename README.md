Here are TMUDFs using Solr build-in to provide full text search capabilities.<br/>

Solr used is v6.6.0.
There needs add solrj related jars also since there use yaml formatted config file,there should add yaml related jars, others are log files . following is the maven dependency.<br/>
```xml
<dependency>
    <groupId>org.apache.solr</groupId>
    <artifactId>solr-solrj</artifactId>
    <version>6.6.0</version>
</dependency>

<dependency>
    <groupId>org.yaml</groupId>
  <artifactId>snakeyaml</artifactId>
  <version>1.15</version>
</dependency>

<dependency>
    <groupId>org.slf4j</groupId>
  <artifactId>slf4j-log4j12</artifactId>
  <version>1.7.5</version>
</dependency>
```

This TMUDF support distributed model based on SolrCloud model, and SolrCloud work based on zookeeper, so There need provide a yaml formatted file to give zkhosts, and other necessary message. An example of yaml content named solr.yaml as follow:<br/>
```
zkhosts : 
- 192.168.0.11:2181
defaultCollection : traf_collection
schema : SEABASE
catalog : TRAFODION
```
This solr.yaml shold put in $TRAF_HOME/udr/public/external_libs <br/>
By using SolrCloud model there should has it's own config for create a collection. following is an example to create a config and then create a collection:

* create config named traf_config
```
${SOLR_HOME}/server/scripts/cloud-scripts/zkcli.sh -zkhost localhost:2181 -cmd upconfig -confdir ${SOLR_HOME}/server/solr/configsets/sample_techproducts_configs/conf -confname traf_config
```

* create collection named traf_config
```
http://localhost:8983/solr/admin/collections?action=CREATE&name=traf_collection&maxShardsPerNode=2&numShards=1&replicationFactor=2&collection.configName=traf_config
```

if user want to support Chinese full text search, there should add Chinese participle jar and field type & dynamic field into solr schema file.

* add Chinese participle jar:
download (https://github.com/mashengchen/ik-analyzer/archive/master.zip) <br />
 do
```
mvn package

cp ${IK_ANALYZER_DIR}/target/ik-analyzer-*.jar ${SOLR_DIR}/server/solr-webapp/webapp/WEB-INF/lib/
cp ${IK_ANALYZER_DIR}/target/classes/*.dic ${SOLR_DIR}/server/solr-webapp/webapp/WEB-INF/classes
cp ${IK_ANALYZER_DIR}/target/classes/*.xml ${SOLR_DIR}/server/solr-webapp/webapp/WEB-INF/classes
```

* add field type & dynamic field <br/>
```
curl -X POST -H 'Content-type:application/json' --data-binary '{
    "add-field-type" : {
        "name":"solr_cnAnalyzer",
        "class":"solr.TextField",
        "positionIncrementGap":"100",
        "indexAnalyzer" : {
            "tokenizer":{ "class":"org.wltea.analyzer.solr.IKTokenizerFactory" }
        },
        "queryAnalyzer" : {
            "tokenizer":{ "class":"org.wltea.analyzer.solr.IKTokenizerFactory" }
        }
    }
    }' http://localhost:8983/solr/${TRAF_COLLECTION}/schema
curl -X POST -H 'Content-type:application/json' --data-binary '{
    "add-dynamic-field":{
        "name":"*_cn_s",
        "type":"solr_cnAnalyzer",
        "indexed":true,
        "stored":false 
    }
    }' http://localhost:8983/solr/${TRAF_COLLECTION}/schema
```


<br/>
The TMUDF "txt_ndx_insert" is for insert datas, while "TXT_SEARCH" is for search.<br/>

For the "txt_ndx_insert", two parameters are expected by the TMUDF: the table name to insert data and cols to specify which col will be full text search and which cols is primary keys, separated by comma.<br/>
note: the first part of param cols is for full text search and follows are primary keys, and currently there just support one col full text search.<br/>

* function ddl:
```sql
create table_mapping function txt_ndx_insert(tblName varchar(100) not null, cols varchar(100))
   external name 'org.trafodion.udf.txt_ndx_insert' -- name of your class
   language java
   library SolrUDFlib;
```

* example for usage:
```sql
select *  
FROM UDF (txt_ndx_insert (TABLE 
        (SELECT name, id ,type
            FROM (INSERT INTO tt (id,type,name) VALUES (11, 'F', 'this is sentence one'),(10, 'A', 'this is sentence two')) tt
         ), 'tt', 'name, id ,type'));
```

For the "TXT_SEARCH", three parameters are expected by the TMUDF: the table name to search the data and col to specify which col will be full text search and the search words.<br/>
note: currently there just support one col full text search.

* function ddl:
```sql
create table_mapping function TXT_SEARCH(tblName varchar(100) not null, col varchar(100), keyword varchar(1000) CHARACTER SET ucs2)
   external name 'org.trafodion.udf.TXT_SEARCH' -- name of your class
   language java
   library SolrUDFlib;
```

* example for usage:
```sql
SELECT * FROM UDF(TXT_SEARCH('tt','name', 'sentence'));
```

As of Traf 2.3, the dependency jars can put under $TRAF_HOME/udr/public/external_libs, and there will load the jars under the directory first.<br/>


