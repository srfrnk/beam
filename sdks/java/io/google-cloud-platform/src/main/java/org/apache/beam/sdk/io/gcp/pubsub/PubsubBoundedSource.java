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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.api.client.util.Clock;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Users should use {@link PubsubIO#read} instead.
 *
 * <p>A PTransform which streams messages from Pubsub.
 *
 * <ul>
 *   <li>The underlying implementation in an {@link BoundedSource} which receives messages in
 *       batches and hands them out one at a time.
 *   <li>The watermark (either in Pubsub processing time or custom timestamp time) is estimated by
 *       keeping track of the minimum of the last minutes worth of messages. This assumes Pubsub
 *       delivers the oldest (in Pubsub processing time) available message at least once a minute,
 *       and that custom timestamps are 'mostly' monotonic with Pubsub processing time.
 *       Unfortunately both of those assumptions are fragile. Thus the estimated watermark may get
 *       ahead of the 'true' watermark and cause some messages to be late.
 *   <li>Checkpoints are used both to ACK received messages back to Pubsub (so that they may be
 *       retired on the Pubsub end), and to NACK already consumed messages should a checkpoint need
 *       to be restored (so that Pubsub will resend those messages promptly).
 *   <li>The backlog is determined by each reader using the messages which have been pulled from
 *       Pubsub but not yet consumed downstream. The backlog does not take account of any messages
 *       queued by Pubsub for the subscription. Unfortunately there is currently no API to determine
 *       the size of the Pubsub queue's backlog.
 *   <li>The subscription must already exist.
 *   <li>The subscription timeout is read whenever a reader is started. However it is not checked
 *       thereafter despite the timeout being user-changeable on-the-fly.
 *   <li>We log vital stats every 30 seconds.
 *   <li>Though some background threads may be used by the underlying transport all Pubsub calls are
 *       blocking. We rely on the underlying runner to allow multiple {@link
 *       BoundedSource.BoundedReader} instances to execute concurrently and thus hide latency.
 * </ul>
 */
public class PubsubBoundedSource extends PTransform<PBegin, PCollection<PubsubMessage>> {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubBoundedSource.class);
  private final PubsubUnboundedSource pubsubUnboundedSource;
  private final long maxNumRecords;
  private final @Nullable Duration maxReadTime;

  @VisibleForTesting
  PubsubBoundedSource(
      Clock clock,
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes,
      boolean needsMessageId,
      long maxNumRecords,
      @Nullable Duration maxReadTime) {
    pubsubUnboundedSource =
        new PubsubUnboundedSource(
            clock,
            pubsubFactory,
            project,
            topic,
            subscription,
            timestampAttribute,
            idAttribute,
            needsAttributes,
            needsMessageId);
    this.maxNumRecords = maxNumRecords;
    this.maxReadTime = maxReadTime;
  }

  /** Construct an bounded source to consume from the Pubsub {@code subscription}. */
  public PubsubBoundedSource(
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes,
      long maxNumRecords,
      @Nullable Duration maxReadTime) {
    this(
        null,
        pubsubFactory,
        project,
        topic,
        subscription,
        timestampAttribute,
        idAttribute,
        needsAttributes,
        false,
        maxNumRecords,
        maxReadTime);
  }

  /** Construct an bounded source to consume from the Pubsub {@code subscription}. */
  public PubsubBoundedSource(
      Clock clock,
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes,
      long maxNumRecords,
      @Nullable Duration maxReadTime) {
    this(
        clock,
        pubsubFactory,
        project,
        topic,
        subscription,
        timestampAttribute,
        idAttribute,
        needsAttributes,
        false,
        maxNumRecords,
        maxReadTime);
  }

  /** Construct an bounded source to consume from the Pubsub {@code subscription}. */
  public PubsubBoundedSource(
      PubsubClientFactory pubsubFactory,
      @Nullable ValueProvider<ProjectPath> project,
      @Nullable ValueProvider<TopicPath> topic,
      @Nullable ValueProvider<SubscriptionPath> subscription,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      boolean needsAttributes,
      boolean needsMessageId,
      long maxNumRecords,
      @Nullable Duration maxReadTime) {
    this(
        null,
        pubsubFactory,
        project,
        topic,
        subscription,
        timestampAttribute,
        idAttribute,
        needsAttributes,
        needsMessageId,
        maxNumRecords,
        maxReadTime);
  }

  @Override
  public PCollection<PubsubMessage> expand(PBegin input) {
    return input
        .getPipeline()
        .begin()
        .apply(
            Read.from(new PubsubUnboundedSource.PubsubSource(this.pubsubUnboundedSource))
                .withMaxNumRecords(this.maxNumRecords)
                .withMaxReadTime(this.maxReadTime));
  }
}
