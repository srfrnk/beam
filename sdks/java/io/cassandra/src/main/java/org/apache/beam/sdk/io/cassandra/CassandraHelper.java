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
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import org.apache.beam.sdk.options.ValueProvider;
import java.util.List;

final class CassandraHelper {
  public static Cluster getCluster(ValueProvider<List<String>> hosts, ValueProvider<Integer> port,
      ValueProvider<String> username, ValueProvider<String> password, ValueProvider<String> localDc,
      ValueProvider<String> consistencyLevel) {
    Cluster.Builder builder =
        Cluster.builder().addContactPoints(hosts.get().toArray(new String[0])).withPort(port.get());

    if (username != null) {
      builder.withAuthProvider(new PlainTextAuthProvider(username.get(), password.get()));
    }

    DCAwareRoundRobinPolicy.Builder dcAwarePolicyBuilder = new DCAwareRoundRobinPolicy.Builder();
    if (localDc != null) {
      dcAwarePolicyBuilder.withLocalDc(localDc.get());
    }

    builder.withLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicyBuilder.build()));

    if (consistencyLevel != null) {
      builder.withQueryOptions(
          new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel.get())));
    }

    return builder.build();
  }
}
