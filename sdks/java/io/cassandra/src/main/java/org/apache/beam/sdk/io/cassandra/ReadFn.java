/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import java.io.Closeable;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadFn<OutputT> extends DoFn<String, OutputT> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadFn.class);

  private final List<String> hosts;
  private final int port;
  private final String username;
  private final String password;
  private final String localDc;
  private final String consistencyLevel;
  private final String keyspace;
  private final Class<OutputT> entity;

  public ReadFn(
      List<String> hosts,
      int port,
      String username,
      String password,
      String localDc,
      String consistencyLevel,
      String keyspace,
      Class<OutputT> entity) {
    this.hosts = hosts;
    this.port = port;
    this.username = username;
    this.password = password;
    this.localDc = localDc;
    this.consistencyLevel = consistencyLevel;
    this.keyspace = keyspace;
    this.entity = entity;
  }

  Cluster cluster;
  Session session;

  @Setup
  public void setup() throws Exception {
    cluster =
        CassandraHelper.getCluster(hosts, port, username, password, localDc, consistencyLevel);
    session = cluster.connect(keyspace);
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    MappingManager mappingManager = new MappingManager(session);
    Mapper<OutputT> mapper = mappingManager.mapper(entity);
    String query = context.element();
    LOG.debug("Executing Cassandra Query: " + query);
    ResultSet resultSet = session.execute(query);
    Result<OutputT> results = mapper.map(resultSet);
    for (OutputT result : results) {
      context.output(result);
    }
  }

  @Teardown
  public void teardown() throws Exception {
    if (session != null) {
      ((Closeable) session).close();
    }
    if (cluster != null) {
      ((Closeable) cluster).close();
    }
  }
}
