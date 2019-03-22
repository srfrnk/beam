/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.beam.sdk.io.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SplitReadFn extends DoFn<Void, String> {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadFn.class);
  private static final String MURMUR3PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";

  private final ValueProvider<List<String>> hosts;
  private final ValueProvider<Integer> port;
  private final ValueProvider<String> username;
  private final ValueProvider<String> password;
  private final ValueProvider<String> localDc;
  private final ValueProvider<String> consistencyLevel;
  private final ValueProvider<String> keyspace;
  private final ValueProvider<String> table;
  private final ValueProvider<String> where;

  public SplitReadFn(ValueProvider<List<String>> hosts, ValueProvider<Integer> port,
      ValueProvider<String> username, ValueProvider<String> password, ValueProvider<String> localDc,
      ValueProvider<String> consistencyLevel, ValueProvider<String> keyspace,
      ValueProvider<String> table, ValueProvider<String> where) {
    this.hosts = hosts;
    this.port = port;
    this.username = username;
    this.password = password;
    this.localDc = localDc;
    this.consistencyLevel = consistencyLevel;
    this.keyspace = keyspace;
    this.table = table;
    this.where = where;
  }

  @Setup
  public void setup() throws Exception {
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    try (
        Cluster cluster = CassandraHelper.getCluster(hosts.get(), port.get(), username.get(),
            password.get(), localDc.get(), consistencyLevel.get());
        Session session = cluster.connect(keyspace.get());) {

      if (isMurmur3Partitioner(cluster)) {
        LOG.info("Murmur3Partitioner detected, splitting");
        split(cluster).forEach(query -> {
          context.output(query);
        });
      } else {
        LOG.warn("Only Murmur3Partitioner is supported for splitting");
        String query = generateRangeQuery(keyspace, table, where, "", null, null);
        context.output(query);
      }
    }
  }

  @Teardown
  public void teardown() throws Exception {
  }

  static boolean isMurmur3Partitioner(Cluster cluster) {
    return MURMUR3PARTITIONER.equals(cluster.getMetadata().getPartitioner());
  }

  private Stream<String> split(Cluster cluster) {
    long numSplits = cluster.getMetrics().getKnownHosts().getValue();
    LOG.info("Number of desired splits is {}", numSplits);

    SplitGenerator splitGenerator = new SplitGenerator(cluster.getMetadata().getPartitioner());
    List<BigInteger> tokens = cluster.getMetadata().getTokenRanges().stream()
        .map(tokenRange -> new BigInteger(tokenRange.getEnd().getValue().toString()))
        .collect(Collectors.toList());
    List<List<RingRange>> splits = splitGenerator.generateSplits(numSplits, tokens);
    LOG.info("{} splits were actually generated", splits.size());

    final String partitionKey =
        cluster.getMetadata().getKeyspace(keyspace.get()).getTable(table.get()).getPartitionKey()
            .stream().map(ColumnMetadata::getName).collect(Collectors.joining(","));

    return splits.stream().flatMap(split -> split.stream())
        .flatMap(range -> getSplitForRange(range, partitionKey));
  }

  private Stream<String> getSplitForRange(RingRange range, String partitionKey) {
    if (range.isWrapping()) {
      // A wrapping range is one that overlaps from the end of the partitioner range
      // and its
      // start (ie : when the start token of the split is greater than the end token)
      // We need to generate two queries here : one that goes from the start token to
      // the end of
      // the partitioner range, and the other from the start of the partitioner range
      // to the
      // end token of the split.
      return Stream.of(
          generateRangeQuery(keyspace, table, where, partitionKey, range.getStart(), null),
          generateRangeQuery(keyspace, table, where, partitionKey, null, range.getEnd()));
    } else {
      return Stream.of(generateRangeQuery(keyspace, table, where, partitionKey, range.getStart(),
          range.getEnd()));
    }
  }

  private static String generateRangeQuery(ValueProvider<String> keyspace,
      ValueProvider<String> table, ValueProvider<String> where, String partitionKey,
      BigInteger rangeStart, BigInteger rangeEnd) {
    String query = String.format("SELECT * FROM %s.%s WHERE %s;", keyspace.get(), table.get(),
        Joiner.on(" AND ").skipNulls().join(
            where == null ? null : String.format("(%s)", where.get()),
            rangeStart == null ? null : String.format("(token(%s)>=%d)", partitionKey, rangeStart),
            rangeEnd == null ? null : String.format("(token(%s)<%d)", partitionKey, rangeEnd)));
    LOG.debug("Cassandra generated read query : {}", query);
    return query;
  }
}
