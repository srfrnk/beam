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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.auto.value.AutoValue;
import com.google.common.util.concurrent.ListenableFuture;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to read from Apache Cassandra.
 *
 * <h3>Reading from Apache Cassandra</h3>
 *
 * <p>
 * {@code CassandraIO} provides a source to read and returns a bounded collection of entities as
 * {@code PCollection<Entity>}. An entity is built by Cassandra mapper ({@code
 * com.datastax.driver.mapping.EntityMapper}) based on a POJO containing annotations (as described
 * http://docs.datastax .com/en/developer/java-driver/2.1/manual/object_mapper/creating/").
 *
 * <p>
 * The following example illustrates various options for configuring the IO:
 *
 * <pre>
 * {@code
 * pipeline.apply(CassandraIO.<Person>read()
 *     .withHosts(Arrays.asList("host1", "host2"))
 *     .withPort(9042)
 *     .withKeyspace("beam")
 *     .withTable("Person")
 *     .withEntity(Person.class)
 *     .withCoder(SerializableCoder.of(Person.class))
 *     // above options are the minimum set, returns PCollection<Person>
 *
 * }
 * </pre>
 *
 * <h3>Writing to Apache Cassandra</h3>
 *
 * <p>
 * {@code CassandraIO} provides a sink to write a collection of entities to Apache Cassandra.
 *
 * <p>
 * The following example illustrates various options for configuring the IO write:
 *
 * <pre>
 * {@code
 * pipeline
 *    .apply(...) // provides a PCollection<Person> where Person is an entity
 *    .apply(CassandraIO.<Person>write()
 *        .withHosts(Arrays.asList("host1", "host2"))
 *        .withPort(9042)
 *        .withKeyspace("beam")
 *        .withEntity(Person.class));
 * }
 * </pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CassandraIO {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

  private CassandraIO() {
  }

  /** Provide a {@link Read} {@link PTransform} to read data from a Cassandra database. */
  public static <T> Read<T> read() {
    return new AutoValue_CassandraIO_Read.Builder<T>().build();
  }

  /** Provide a {@link Write} {@link PTransform} to write data to a Cassandra database. */
  public static <T> Write<T> write() {
    return Write.<T>builder(MutationType.WRITE).build();
  }

  /** Provide a {@link Write} {@link PTransform} to delete data to a Cassandra database. */
  public static <T> Write<T> delete() {
    return Write.<T>builder(MutationType.DELETE).build();
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract ValueProvider<List<String>> hosts();

    @Nullable
    abstract ValueProvider<Integer> port();

    @Nullable
    abstract ValueProvider<String> keyspace();

    @Nullable
    abstract ValueProvider<String> table();

    @Nullable
    abstract Class<T> entity();

    @Nullable
    abstract Coder<T> coder();

    @Nullable
    abstract ValueProvider<String> username();

    @Nullable
    abstract ValueProvider<String> password();

    @Nullable
    abstract ValueProvider<String> localDc();

    @Nullable
    abstract ValueProvider<String> consistencyLevel();

    @Nullable
    abstract ValueProvider<String> where();

    @Nullable
    abstract ValueProvider<Integer> minNumberOfSplits();

    abstract Builder<T> builder();

    /** Specify the hosts of the Apache Cassandra instances. */
    public Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return builder().setHosts(ValueProvider.StaticValueProvider.of(hosts)).build();
    }

    /** Specify the hosts of the Apache Cassandra instances. */
    public Read<T> withHosts(ValueProvider<List<String>> hosts) {
      return builder().setHosts(hosts).build();
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public Read<T> withPort(int port) {
      checkArgument(port > 0, "port must be > 0, but was: %s", port);
      return builder().setPort(ValueProvider.StaticValueProvider.of(port)).build();
    }

    /** Specify the port number of the Apache Cassandra instances. */
    public Read<T> withPort(ValueProvider<Integer> port) {
      return builder().setPort(port).build();
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "keyspace can not be null");
      return builder().setKeyspace(ValueProvider.StaticValueProvider.of(keyspace)).build();
    }

    /** Specify the Cassandra keyspace where to read data. */
    public Read<T> withKeyspace(ValueProvider<String> keyspace) {
      return builder().setKeyspace(keyspace).build();
    }

    /** Specify the Cassandra table where to read data. */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "table can not be null");
      return builder().setTable(ValueProvider.StaticValueProvider.of(table)).build();
    }

    /** Specify the Cassandra table where to read data. */
    public Read<T> withTable(ValueProvider<String> table) {
      return builder().setTable(table).build();
    }

    /**
     * Specify the entity class (annotated POJO). The {@link CassandraIO} will read the data and
     * convert the data as entity instances. The {@link PCollection} resulting from the read will
     * contains entity elements.
     */
    public Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    /** Specify the {@link Coder} used to serialize the entity in the {@link PCollection}. */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    /** Specify the username for authentication. */
    public Read<T> withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(ValueProvider.StaticValueProvider.of(username)).build();
    }

    /** Specify the username for authentication. */
    public Read<T> withUsername(ValueProvider<String> username) {
      return builder().setUsername(username).build();
    }

    /** Specify the password for authentication. */
    public Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(ValueProvider.StaticValueProvider.of(password)).build();
    }

    /** Specify the clear password for authentication. */
    public Read<T> withPassword(ValueProvider<String> password) {
      return builder().setPassword(password).build();
    }

    /** Specify the local DC used for the load balancing. */
    public Read<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "localDc can not be null");
      return builder().setLocalDc(ValueProvider.StaticValueProvider.of(localDc)).build();
    }

    /** Specify the local DC used for the load balancing. */
    public Read<T> withLocalDc(ValueProvider<String> localDc) {
      return builder().setLocalDc(localDc).build();
    }

    public Read<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
      return builder().setConsistencyLevel(ValueProvider.StaticValueProvider.of(consistencyLevel))
          .build();
    }

    public Read<T> withConsistencyLevel(ValueProvider<String> consistencyLevel) {
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    /**
     * Specify a string with a partial {@code Where} clause. Note: Cassandra places restrictions on
     * the {@code Where} clause you may use. (e.g. filter on a primary/clustering column only etc.)
     *
     * @param where Partial {@code Where} clause. Optional - If unspecified will not filter the
     *              data.
     * @see <a href="http://cassandra.apache.org/doc/4.0/cql/dml.html#where-clause">CQL
     *      Documentation</a>
     * @throws com.datastax.driver.core.exceptions.InvalidQueryException If {@code Where} clause
     *                                                                   makes the generated query
     *                                                                   invalid. Please Consult
     *                                                                   <a href=
     *                                                                   "http://cassandra.apache.org/doc/4.0/cql/dml.html#where-clause">CQL
     *                                                                   Documentation</a> for more
     *                                                                   info on correct usage of
     *                                                                   the {@code Where} clause.
     */
    public Read<T> withWhere(String where) {
      checkArgument(where != null, "where can not be null");
      return builder().setWhere(ValueProvider.StaticValueProvider.of(where)).build();
    }

    /**
     * Specify a string with a partial {@code Where} clause. Note: Cassandra places restrictions on
     * the {@code Where} clause you may use. (e.g. filter on a primary/clustering column only etc.)
     *
     * @param where Partial {@code Where} clause. Optional - If unspecified will not filter the
     *              data.
     * @see <a href="http://cassandra.apache.org/doc/4.0/cql/dml.html#where-clause">CQL
     *      Documentation</a>
     * @throws com.datastax.driver.core.exceptions.InvalidQueryException If {@code Where} clause
     *                                                                   makes the generated query
     *                                                                   invalid. Please Consult
     *                                                                   <a href=
     *                                                                   "http://cassandra.apache.org/doc/4.0/cql/dml.html#where-clause">CQL
     *                                                                   Documentation</a> for more
     *                                                                   info on correct usage of
     *                                                                   the {@code Where} clause.
     */
    public Read<T> withWhere(ValueProvider<String> where) {
      return builder().setWhere(where).build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read<T> withMinNumberOfSplits(Integer minNumberOfSplits) {
      checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
      checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
      return builder().setMinNumberOfSplits(ValueProvider.StaticValueProvider.of(minNumberOfSplits))
          .build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read<T> withMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits) {
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument((hosts() != null && port() != null), "WithHosts() and withPort() are required");
      checkArgument(keyspace() != null, "withKeyspace() is required");
      checkArgument(table() != null, "withTable() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

      return input.apply("Initiate", Create.of((Void) null))
          .apply("Create query splits",
              ParDo.of(new SplitReadFn(hosts(), port(), username(), password(), localDc(),
                  consistencyLevel(), keyspace(), table(), where())))
          .apply("Read Casandra", ParDo.of(new ReadFn<T>(hosts(), port(), username(), password(),
              localDc(), consistencyLevel(), keyspace(), entity())))
          .setCoder(coder());
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(ValueProvider<List<String>> hosts);

      abstract Builder<T> setPort(ValueProvider<Integer> port);

      abstract Builder<T> setKeyspace(ValueProvider<String> keyspace);

      abstract Builder<T> setTable(ValueProvider<String> table);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setUsername(ValueProvider<String> username);

      abstract Builder<T> setPassword(ValueProvider<String> password);

      abstract Builder<T> setLocalDc(ValueProvider<String> localDc);

      abstract Builder<T> setConsistencyLevel(ValueProvider<String> consistencyLevel);

      abstract Builder<T> setWhere(ValueProvider<String> where);

      abstract Builder<T> setMinNumberOfSplits(ValueProvider<Integer> minNumberOfSplits);

      abstract Read<T> build();
    }
  }

  /** Specify the mutation type: either write or delete. */
  public enum MutationType {
    WRITE, DELETE
  }

  /**
   * A {@link PTransform} to mutate into Apache Cassandra. See {@link CassandraIO} for details on
   * usage and configuration.
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer port();

    @Nullable
    abstract String keyspace();

    @Nullable
    abstract Class<T> entity();

    @Nullable
    abstract String username();

    @Nullable
    abstract String password();

    @Nullable
    abstract String localDc();

    @Nullable
    abstract String consistencyLevel();

    abstract MutationType mutationType();

    abstract Builder<T> builder();

    static <T> Builder<T> builder(MutationType mutationType) {
      return new AutoValue_CassandraIO_Write.Builder<T>().setMutationType(mutationType);
    }

    /** Specify the Cassandra instance hosts where to write data. */
    public Write<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null,
          "CassandraIO." + getMutationTypeName() + "().withHosts(hosts) called with null hosts");
      checkArgument(!hosts.isEmpty(), "CassandraIO." + getMutationTypeName()
          + "().withHosts(hosts) called with empty " + "hosts list");
      return builder().setHosts(hosts).build();
    }

    /** Specify the Cassandra instance port number where to write data. */
    public Write<T> withPort(int port) {
      checkArgument(port > 0, "CassandraIO." + getMutationTypeName()
          + "().withPort(port) called with invalid port " + "number (%s)", port);
      return builder().setPort(port).build();
    }

    /** Specify the Cassandra keyspace where to write data. */
    public Write<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "CassandraIO." + getMutationTypeName()
          + "().withKeyspace(keyspace) called with " + "null keyspace");
      return builder().setKeyspace(keyspace).build();
    }

    /**
     * Specify the entity class in the input {@link PCollection}. The {@link CassandraIO} will map
     * this entity to the Cassandra table thanks to the annotations.
     */
    public Write<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "CassandraIO." + getMutationTypeName()
          + "().withEntity(entity) called with null " + "entity");
      return builder().setEntity(entity).build();
    }

    /** Specify the username used for authentication. */
    public Write<T> withUsername(String username) {
      checkArgument(username != null, "CassandraIO." + getMutationTypeName()
          + "().withUsername(username) called with " + "null username");
      return builder().setUsername(username).build();
    }

    /** Specify the password used for authentication. */
    public Write<T> withPassword(String password) {
      checkArgument(password != null, "CassandraIO." + getMutationTypeName()
          + "().withPassword(password) called with " + "null password");
      return builder().setPassword(password).build();
    }

    /** Specify the local DC used by the load balancing policy. */
    public Write<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "CassandraIO." + getMutationTypeName()
          + "().withLocalDc(localDc) called with null" + " localDc");
      return builder().setLocalDc(localDc).build();
    }

    public Write<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "CassandraIO." + getMutationTypeName()
          + "().withConsistencyLevel" + "(consistencyLevel) called with null consistencyLevel");
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(hosts() != null, "CassandraIO." + getMutationTypeName()
          + "() requires a list of hosts to be set via withHosts(hosts)");
      checkState(port() != null, "CassandraIO." + getMutationTypeName() + "() requires a "
          + "valid port number to be set via withPort(port)");
      checkState(keyspace() != null, "CassandraIO." + getMutationTypeName()
          + "() requires a keyspace to be set via " + "withKeyspace(keyspace)");
      checkState(entity() != null, "CassandraIO." + getMutationTypeName()
          + "() requires an entity to be set via " + "withEntity(entity)");
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (mutationType() == MutationType.DELETE) {
        input.apply(ParDo.of(new DeleteFn<>(this)));
      } else {
        input.apply(ParDo.of(new WriteFn<>(this)));
      }
      return PDone.in(input.getPipeline());
    }

    private String getMutationTypeName() {
      return mutationType() == null ? MutationType.WRITE.name().toLowerCase()
          : mutationType().name().toLowerCase();
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(List<String> hosts);

      abstract Builder<T> setPort(Integer port);

      abstract Builder<T> setKeyspace(String keyspace);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Builder<T> setUsername(String username);

      abstract Builder<T> setPassword(String password);

      abstract Builder<T> setLocalDc(String localDc);

      abstract Builder<T> setConsistencyLevel(String consistencyLevel);

      abstract Builder<T> setMutationType(MutationType mutationType);

      abstract Write<T> build();
    }
  }

  private static class WriteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> writer;

    WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      writer = new Mutator<>(spec, Mapper::saveAsync, "writes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      writer.mutate(c.element());
    }

    @Teardown
    public void teardown() throws Exception {
      writer.close();
      writer = null;
    }
  }

  private static class DeleteFn<T> extends DoFn<T, Void> {
    private final Write<T> spec;
    private transient Mutator<T> deleter;

    DeleteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() {
      deleter = new Mutator<>(spec, Mapper::deleteAsync, "deletes");
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws ExecutionException, InterruptedException {
      deleter.mutate(c.element());
    }

    @Teardown
    public void teardown() throws Exception {
      deleter.close();
      deleter = null;
    }
  }

  /** Get a Cassandra cluster using hosts and port. */
  private static Cluster getCluster(List<String> hosts, int port, String username, String password,
      String localDc, String consistencyLevel) {
    Cluster.Builder builder =
        Cluster.builder().addContactPoints(hosts.toArray(new String[0])).withPort(port);

    if (username != null) {
      builder.withAuthProvider(new PlainTextAuthProvider(username, password));
    }

    DCAwareRoundRobinPolicy.Builder dcAwarePolicyBuilder = new DCAwareRoundRobinPolicy.Builder();
    if (localDc != null) {
      dcAwarePolicyBuilder.withLocalDc(localDc);
    }

    builder.withLoadBalancingPolicy(new TokenAwarePolicy(dcAwarePolicyBuilder.build()));

    if (consistencyLevel != null) {
      builder.withQueryOptions(
          new QueryOptions().setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel)));
    }

    return builder.build();
  }

  private static Cluster getCluster(ValueProvider<List<String>> hosts, ValueProvider<Integer> port,
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

  /** Mutator allowing to do side effects into Apache Cassandra database. */
  private static class Mutator<T> {
    /**
     * The threshold of 100 concurrent async queries is a heuristic commonly used by the Apache
     * Cassandra community. There is no real gain to expect in tuning this value.
     */
    private static final int CONCURRENT_ASYNC_QUERIES = 100;

    private final Cluster cluster;
    private final Session session;
    private final MappingManager mappingManager;
    private List<ListenableFuture<Void>> mutateFutures;
    private final BiFunction<Mapper<T>, T, ListenableFuture<Void>> mutator;
    private final String operationName;

    Mutator(Write<T> spec, BiFunction<Mapper<T>, T, ListenableFuture<Void>> mutator,
        String operationName) {
      this.cluster = getCluster(spec.hosts(), spec.port(), spec.username(), spec.password(),
          spec.localDc(), spec.consistencyLevel());
      this.session = cluster.connect(spec.keyspace());
      this.mappingManager = new MappingManager(session);
      this.mutateFutures = new ArrayList<>();
      this.mutator = mutator;
      this.operationName = operationName;
    }

    /**
     * Mutate the entity to the Cassandra instance, using {@link Mapper} obtained with the
     * {@link MappingManager}. This method uses {@link Mapper#saveAsync(Object)} method, which is
     * asynchronous. Beam will wait for all futures to complete, to guarantee all writes have
     * succeeded.
     */
    void mutate(T entity) throws ExecutionException, InterruptedException {
      Mapper<T> mapper = (Mapper<T>) mappingManager.mapper(entity.getClass());
      this.mutateFutures.add(mutator.apply(mapper, entity));
      if (this.mutateFutures.size() == CONCURRENT_ASYNC_QUERIES) {
        // We reached the max number of allowed in flight queries.
        // Write methods are synchronous in Beam,
        // so we wait for each async query to return before exiting.
        LOG.debug("Waiting for a batch of {} Cassandra {} to be executed...",
            CONCURRENT_ASYNC_QUERIES, operationName);
        waitForFuturesToFinish();
        this.mutateFutures = new ArrayList<>();
      }
    }

    void close() throws ExecutionException, InterruptedException {
      if (this.mutateFutures.size() > 0) {
        // Waiting for the last in flight async queries to return before finishing the bundle.
        waitForFuturesToFinish();
      }

      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    }

    private void waitForFuturesToFinish() throws ExecutionException, InterruptedException {
      for (ListenableFuture<Void> future : mutateFutures) {
        future.get();
      }
    }
  }
}
