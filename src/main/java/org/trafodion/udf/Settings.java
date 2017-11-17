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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class Settings {
    public int batch = 100;
    public String defaultCollection = null;
    private String schema = null;
    private String catalog = null;

    public static Settings read(final InputStream stream) {
        Objects.requireNonNull(stream);

        final Constructor constructor = new Constructor(Settings.class);
        final TypeDescription settingsDescription = new TypeDescription(Settings.class);
        settingsDescription.putListPropertyType("zkhosts", String.class);
        constructor.addTypeDescription(settingsDescription);

        final Yaml yaml = new Yaml(constructor);
        return yaml.loadAs(stream, Settings.class);
    }

    public String getZkStr() {
        if (System.getenv("ZOO_PORT_NODES") != null && !"".equals(System.getenv("ZOO_PORT_NODES"))) {
            return System.getenv("ZOO_PORT_NODES");
        } else {
            return "localhost:2181";
        }
    }

    @Override
    public String toString() {
        return "zkhosts : " + getZkStr() + "; defaultCollection : " + defaultCollection + "; cnField : " + "; ";
    }

    public static void main(String[] args) {
        try {
            Settings s = Settings.read(new FileInputStream(new File("remote-objects.yaml")));

            System.out.println(s);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public String getSchema() {
        return schema == null ? "SEABASE" : schema;
    }

    public void setSchema(String schema) {
        this.schema = schema.toUpperCase();
    }

    public String getCatalog() {
        return catalog == null ? "TRAFODION" : catalog;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog.toUpperCase();
    }

}