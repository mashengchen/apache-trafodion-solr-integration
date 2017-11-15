SOLR_UDF_JARNAME = SolrUDF-1.0.jar
SOLR_INTEGRATION_CONFIG=solr.yaml

all: build
build:
	echo "$(MAVEN) clean package -DskipTests"
	set -o pipefail && $(MAVEN) clean package -DskipTests | tee -a build_solrudf.log
	cp target/$(SOLR_UDF_JARNAME) $(MY_SQROOT)/export/lib
	@if [ ! -d $(MY_SQROOT)/udr/public/external_libs ] ; then mkdir -p $(MY_SQROOT)/udr/public/external_libs; fi
	cp remote-objects.yaml $(MY_SQROOT)/udr/public/external_libs/$(SOLR_INTEGRATION_CONFIG)
clean:
	$(RM) $(MY_SQROOT)/export/lib/$(SOLR_UDF_JARNAME)
	-$(MAVEN) -f pom.xml clean | grep ERROR
