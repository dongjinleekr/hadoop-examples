/*
 * Copyright (C) 2012 Dongjin Lee (dongjin.lee.kr@gmail.com)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.github.dongjinleekr.hadoop.examples;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * An example class which demonstrates how to use the Distributed Cache feature of hadoop, for
 * har-formatted file.
 * 
 * The main contents of this example is printCachePath() method. You can make use of this example in
 * Mapper.setup(Context context). (Configuration object can be derived from Context object.)
 * 
 * However, I strongly recommend you to use SequenceFile instead of har file for hadoop-related
 * storage format, including Distributed Cache.
 * 
 * How to run:
 * 
 * <p>
 * mvn compile
 * <p>
 * mvn exec:java -Dexec.mainClass="com.github.dongjinleekr.hadoop.examples.DistributedCacheExample"
 * -D mapred.cache.archives=/home/example.har
 * 
 * How to create har file:
 * 
 * <p>
 * hadoop archive -archiveName [har-file-name] -p [destination] [target-directory]
 * [target-directory-parent]
 * <p>
 * ex) hadoop archive -archiveName example.har -p /home examples /home # archives /home/examples/*
 * into /home/examples.har
 * 
 * @author Dongjin Lee
 */
public class DistributedCacheExample {
  public static void main(String[] args) throws IOException, URISyntaxException {
    GenericOptionsParser parser = new GenericOptionsParser(new Configuration(), args);
    Configuration conf = parser.getConfiguration();

    printCachePath(conf);
  }

  public static void printCachePath(Configuration conf) throws IOException, URISyntaxException {
    FileSystem fs = FileSystem.get(conf);
    URI[] archives = DistributedCache.getCacheArchives(conf);

    for (URI archive : archives) {
      HarFileSystem hfs = new HarFileSystem();
      String cacheUri =
          String.format("har://hdfs-%s:%d%s", fs.getUri().getHost(), fs.getUri().getPort(),
              archive.toString());
      System.out.println(cacheUri);

      hfs.initialize(new URI(cacheUri), conf);

      FileStatus root = hfs.listStatus(new Path("."))[0];
      FileStatus[] children = hfs.listStatus(root.getPath());

      for (FileStatus child : children) {
        System.out.println(child.getPath());
      }

      IOUtils.closeStream(hfs);
    }
  }
}
