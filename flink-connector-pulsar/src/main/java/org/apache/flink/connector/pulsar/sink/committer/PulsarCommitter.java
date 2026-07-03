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

package org.apache.flink.connector.pulsar.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorNotFoundException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.InvalidTxnStatusException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.naming.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.getTcClient;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_ABORTED;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_UNCOMMITTED;
import static org.apache.pulsar.common.naming.SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN;

/**
 * Committer implementation for {@link PulsarSink}.
 *
 * <p>The committer is responsible to finalize the Pulsar transactions by committing them.
 */
@Internal
public class PulsarCommitter implements Committer<PulsarCommittable>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarCommitter.class);

    private final SinkConfiguration sinkConfiguration;

    private PulsarClient pulsarClient;
    private PulsarAdmin pulsarAdmin;
    private TransactionCoordinatorClient coordinatorClient;

    public PulsarCommitter(
            SinkConfiguration sinkConfiguration, CommitterInitContext committerInitContext) {
        this.sinkConfiguration = checkNotNull(sinkConfiguration);
    }

    @Override
    @SuppressWarnings("java:S3776")
    public void commit(Collection<CommitRequest<PulsarCommittable>> requests)
            throws PulsarClientException {
        // Case: no transaction created.
        if (requests.isEmpty()) {
            return;
        }
        // Older version data that was stored in the checkpoint.
        // Old versions created one committable per topic per checkpoint. Those checkpoints
        // cannot guarantee exactly-once and are no longer improved.
        TransactionCoordinatorClient client = transactionCoordinatorClient();
        CommitRequest<PulsarCommittable> firstRequest = requests.iterator().next();
        PulsarCommittable firstCommittable = firstRequest.getCommittable();
        if (requests.size() > 1 || firstCommittable.getVersion() == 1) {
            commitMultipleTransactions(requests);
            return;
        }

        // Newest version data.
        TxnID txnID = firstCommittable.getTxnID();
        // No messages were published.
        Map<String, MessageIdPojo> latestMsgIdMap = firstCommittable.getLatestPublishedMessages();
        if (latestMsgIdMap.isEmpty()) {
            try {
                client.commit(txnID);
            } catch (Exception e) {
                // Since no message relates to the transaction, we can ignore the error.
                LOG.warn("Failed to commit an empty transaction. {}", firstCommittable, e);
                firstRequest.signalAlreadyCommitted();
            }
            return;
        }

        // All topics in this transaction share the same txnID. Committing the txnID
        // atomically commits all topics — we only need to successfully query the message
        // state for one topic to decide whether to commit. If getMessagesById fails for
        // a topic, we can safely skip it and try the next one; a successful commit on any
        // topic will cover the entire transaction.
        int messagesFailedQuery = 0;
        for (Map.Entry<String, MessageIdPojo> topicMsgPair : latestMsgIdMap.entrySet()) {
            String topic = topicMsgPair.getKey();
            MessageIdPojo messageId = topicMsgPair.getValue();
            TopicName topicNameObj = TopicName.get(topic);
            // When the Pulsar sink auto-creates a topic, it may create a partitioned topic.
            // In that case topicNameObj.isPartitioned() returns false (the topic name string does
            // not contain "-partition-") but messageId.getPartitionIndex() >= 0, so we fall into
            // the else branch to construct the real partition topic name for message lookup.
            String realTopic =
                    (messageId.getPartitionIndex() == -1 || topicNameObj.isPartitioned())
                            ? topic
                            : topicNameObj.getPartition(messageId.getPartitionIndex()).toString();
            List<Message<byte[]>> messages = null;
            try {
                messages =
                        pulsarAdmin
                                .topics()
                                .getMessagesById(
                                        realTopic, messageId.getLedgerId(), messageId.getEntryId());
            } catch (PulsarAdminException e) {
                messagesFailedQuery++;
                LOG.warn("{} Failed to query message by ID {}", topic, messageId, e);
                continue;
            }
            if (messages == null || messages.isEmpty()) {
                // The message has expired, try to query the message in the next topic.
                continue;
            }
            // For now, there will only be one message in the client's implementation. Multiple
            // individual messages
            // in batch messages are all in message content and will not be parsed into multiple
            // messages.
            Message msg = messages.get(0);
            Map<String, String> props = msg.getProperties();
            String aborted = props.get(TXN_ABORTED);
            String uncommitted = props.get(TXN_UNCOMMITTED);
            if ("true".equalsIgnoreCase(aborted)) {
                String logMsg =
                        String.format(
                                "The transaction %s has been aborted, relates to %s",
                                txnID, latestMsgIdMap);
                LOG.warn(logMsg);
                firstRequest.signalFailedWithKnownReason(new FlinkRuntimeException(logMsg));
                return;
            }

            if ("true".equalsIgnoreCase(uncommitted)) {
                try {
                    client.commit(txnID);
                } catch (Exception e) {
                    handleError(firstRequest, txnID, firstCommittable, e);
                }
                return;
            } else {
                firstRequest.signalAlreadyCommitted();
                return;
            }
        }
        if (messagesFailedQuery > 0) {
            int maxRecommitTimes = sinkConfiguration.getMaxRecommitTimes();
            if (firstRequest.getNumberOfRetries() < maxRecommitTimes) {
                firstRequest.retryLater();
            } else {
                String logMsg =
                        String.format(
                                "The messages can not be queried by Pulsar Admin Client, please check the"
                                        + " configurations. txnID: %s, messages: %s",
                                txnID, latestMsgIdMap);
                LOG.warn(logMsg);
                firstRequest.signalFailedWithKnownReason(new FlinkRuntimeException(logMsg));
            }
            return;
        }
        String logMsg =
                String.format(
                        "The messages related the transaction %s have all been expired, the transaction"
                                + " can not be commit anymore, relates to %s",
                        txnID, latestMsgIdMap);
        LOG.warn(logMsg);
        firstRequest.signalFailedWithKnownReason(new FlinkRuntimeException(logMsg));
    }

    private void handleError(
            CommitRequest<PulsarCommittable> request,
            TxnID txnID,
            PulsarCommittable committable,
            Exception e) {
        if (e instanceof CoordinatorNotFoundException) {
            LOG.error(
                    "We couldn't find the Transaction Coordinator from Pulsar broker {}. "
                            + "Check your broker configuration.",
                    committable,
                    e);
            request.signalFailedWithKnownReason(e);
        } else if (e instanceof InvalidTxnStatusException) {
            LOG.error(
                    "Unable to commit transaction ({}) because it's in an invalid state. "
                            + "Most likely the transaction has been aborted for some reason. "
                            + "Please check the Pulsar broker logs for more details.",
                    committable,
                    e);
            request.signalAlreadyCommitted();
        } else if (e instanceof TransactionNotFoundException) {
            if (request.getNumberOfRetries() == 0) {
                LOG.error(
                        "Unable to commit transaction ({}) because it's not found on Pulsar broker. "
                                + "Most likely the checkpoint interval exceed the transaction timeout.",
                        committable,
                        e);
                request.signalFailedWithKnownReason(e);
            } else {
                LOG.warn(
                        "We can't find the transaction {} after {} retry committing. "
                                + "This may mean that the transaction have been committed in previous but failed with timeout. "
                                + "So we just mark it as committed.",
                        txnID,
                        request.getNumberOfRetries());
                request.signalAlreadyCommitted();
            }
        } else if (e instanceof MetaStoreHandlerNotExistsException) {
            LOG.error(
                    "We can't find the meta store handler by the mostSigBits from TxnID {}. "
                            + "Did you change the metadata for topic {}?",
                    committable,
                    TRANSACTION_COORDINATOR_ASSIGN,
                    e);
            request.signalFailedWithKnownReason(e);
        } else if (e instanceof TransactionCoordinatorClientException) {
            LOG.error(
                    "Encountered retriable exception while committing transaction {}.",
                    committable,
                    e);
            int maxRecommitTimes = sinkConfiguration.getMaxRecommitTimes();
            if (request.getNumberOfRetries() < maxRecommitTimes) {
                request.retryLater();
            } else {
                String message =
                        String.format(
                                "Failed to commit transaction %s after retrying %d times",
                                txnID, maxRecommitTimes);
                request.signalFailedWithKnownReason(new FlinkRuntimeException(message, e));
            }
        } else {
            LOG.error(
                    "Transaction ({}) encountered unknown error and data could be potentially lost.",
                    committable,
                    e);
            request.signalFailedWithUnknownReason(e);
        }
    }

    public void commitMultipleTransactions(Collection<CommitRequest<PulsarCommittable>> requests)
            throws PulsarClientException {
        TransactionCoordinatorClient client = transactionCoordinatorClient();

        for (CommitRequest<PulsarCommittable> request : requests) {
            PulsarCommittable committable = request.getCommittable();
            TxnID txnID = committable.getTxnID();

            LOG.info("Start committing the Pulsar transaction {}", txnID);
            try {
                client.commit(txnID);
            } catch (Exception e) {
                handleError(request, txnID, committable, e);
            }
        }
    }

    /**
     * Lazy initialize this backend Pulsar client. This committer may not be used in {@link
     * DeliveryGuarantee#NONE} and {@link DeliveryGuarantee#AT_LEAST_ONCE}. So we couldn't create
     * the Pulsar client at first.
     */
    private TransactionCoordinatorClient transactionCoordinatorClient()
            throws PulsarClientException {
        if (coordinatorClient == null) {
            this.pulsarClient = createClient(sinkConfiguration);
            this.pulsarAdmin = createAdmin(sinkConfiguration);
            this.coordinatorClient = getTcClient(pulsarClient);
        }

        return coordinatorClient;
    }

    @Override
    public void close() throws IOException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
    }
}
