/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.namespace.hive3;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/** Common utility methods. */
public class CommonUtil {

  private CommonUtil() {}

  private static final String CATALOG_HADOOP_PREFIX = "hadoop.";

  /**
   * True if {@code properties} contains any {@code hadoop.*} catalog key (Spark {@code
   * spark.sql.catalog.*.hadoop.*}).
   */
  public static boolean hasCatalogHadoopOverrides(Map<String, String> properties) {
    if (properties == null) {
      return false;
    }
    for (String k : properties.keySet()) {
      if (k != null
              && k.length() > CATALOG_HADOOP_PREFIX.length()
              && k.regionMatches(true, 0, CATALOG_HADOOP_PREFIX, 0, CATALOG_HADOOP_PREFIX.length())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Merges Spark per-catalog Hadoop overrides into {@code conf}. These correspond to {@code
   * spark.sql.catalog.&lt;name&gt;.hadoop.*} keys: entries in {@code properties} use the suffix
   * after the catalog prefix, e.g. {@code hadoop.hive.metastore.uris} {@code ->} {@code
   * hive.metastore.uris}.
   */
  public static void applyCatalogHadoopOverrides(
          Configuration conf, Map<String, String> properties) {
    if (properties == null || conf == null) {
      return;
    }
    for (Map.Entry<String, String> e : properties.entrySet()) {
      String k = e.getKey();
      String v = e.getValue();
      if (k == null || v == null) {
        continue;
      }
      if (k.length() > CATALOG_HADOOP_PREFIX.length()
              && k.regionMatches(true, 0, CATALOG_HADOOP_PREFIX, 0, CATALOG_HADOOP_PREFIX.length())) {
        conf.set(k.substring(CATALOG_HADOOP_PREFIX.length()), v);
      }
    }
  }


  public static String formatCurrentStackTrace() {
    StackTraceElement[] stack = Thread.currentThread().getStackTrace();
    StringBuilder sb = new StringBuilder();
    for (int i = 2; i < Math.min(stack.length, 10); i++) {
      sb.append(stack[i].toString()).append("\n");
    }
    return sb.toString();
  }

  public static String makeQualified(String path) {
    if (path == null) {
      return null;
    }
    return path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }
}
