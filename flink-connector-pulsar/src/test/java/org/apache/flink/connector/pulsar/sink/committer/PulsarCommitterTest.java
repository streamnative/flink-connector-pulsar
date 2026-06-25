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

import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_ABORTED;
import static org.apache.pulsar.client.admin.internal.TopicsImpl.TXN_UNCOMMITTED;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for {@link PulsarCommitter}. */
class PulsarCommitterTest {

    private static final String TOPIC = "topic-1";
    private static final long LEDGER_ID = 1L;
    private static final long ENTRY_ID = 1L;

    private Object pulsarAdmin;
    private TransactionCoordinatorClient coordinatorClient;
    private Object topicsMock;

    private SinkConfiguration sinkConfiguration;

    @BeforeEach
    void setUp() throws Exception {
        pulsarAdmin = mock(org.apache.pulsar.client.admin.internal.PulsarAdminImpl.class);
        coordinatorClient = mock(TransactionCoordinatorClient.class);
        topicsMock = mock(org.apache.pulsar.client.admin.Topics.class);
        when(((org.apache.pulsar.client.admin.internal.PulsarAdminImpl) pulsarAdmin).topics())
                .thenReturn((org.apache.pulsar.client.admin.Topics) topicsMock);

        Configuration config = new Configuration();
        config.set(PULSAR_SERVICE_URL, "pulsar://localhost:6650");
        config.setString("pulsar.sink.maxRecommitTimes", "3");
        sinkConfiguration = new SinkConfiguration(config);
    }

    @Test
    void commitWithEmptyRequestsDoesNothing() throws Exception {
        PulsarCommitter committer = createCommitter();
        committer.commit(Collections.emptyList());
    }

    @Test
    void commitWithEmptyLatestPublishedMessagesReturnsEarly() throws Exception {
        PulsarCommitter committer = createCommitter();
        TxnID txnID = randomTxnID();
        PulsarCommittable committable = new PulsarCommittable(txnID);
        CommitRequest<PulsarCommittable> request = mockCommitRequest(committable);

        committer.commit(Collections.singletonList(request));

        verify(request, never()).signalFailedWithKnownReason(any());
        verify(request, never()).signalAlreadyCommitted();
    }

    @Test
    void commitSignalsFailedWhenMessageIsAborted() throws Exception {
        PulsarCommitter committer = createCommitter();
        injectMocks(committer);

        TxnID txnID = randomTxnID();
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(LEDGER_ID, ENTRY_ID, 0, 0);
        Map<String, BatchMessageIdImpl> messages = Collections.singletonMap(TOPIC, msgId);
        PulsarCommittable committable = new PulsarCommittable(txnID, messages);
        CommitRequest<PulsarCommittable> request = mockCommitRequest(committable);

        @SuppressWarnings("unchecked")
        Message<byte[]> mockMessage = mock(Message.class);
        Map<String, String> props = new HashMap<>();
        props.put(TXN_ABORTED, "true");
        when(mockMessage.getProperties()).thenReturn(props);
        List<Message<byte[]>> messageList = buildMessageList(mockMessage);

        when(getTopicsMock().getMessagesById(TOPIC, LEDGER_ID, ENTRY_ID))
                .thenReturn(messageList);

        committer.commit(Collections.singletonList(request));
        verify(request).signalFailedWithKnownReason(any(FlinkRuntimeException.class));
    }

    @Test
    void commitSignalsFailedWhenAllMessagesExpired() throws Exception {
        PulsarCommitter committer = createCommitter();
        injectMocks(committer);

        TxnID txnID = randomTxnID();
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(LEDGER_ID, ENTRY_ID, 0, 0);
        Map<String, BatchMessageIdImpl> messages = Collections.singletonMap(TOPIC, msgId);
        PulsarCommittable committable = new PulsarCommittable(txnID, messages);
        CommitRequest<PulsarCommittable> request = mockCommitRequest(committable);

        when(getTopicsMock().getMessagesById(TOPIC, LEDGER_ID, ENTRY_ID))
                .thenReturn(null);

        committer.commit(Collections.singletonList(request));
        verify(request).signalFailedWithKnownReason(any(FlinkRuntimeException.class));
    }

    @Test
    void commitSignalsFailedWhenPulsarAdminThrowsException() throws Exception {
        PulsarCommitter committer = createCommitter();
        injectMocks(committer);

        TxnID txnID = randomTxnID();
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(LEDGER_ID, ENTRY_ID, 0, 0);
        Map<String, BatchMessageIdImpl> messages = Collections.singletonMap(TOPIC, msgId);
        PulsarCommittable committable = new PulsarCommittable(txnID, messages);
        CommitRequest<PulsarCommittable> request = mockCommitRequest(committable);

        when(getTopicsMock().getMessagesById(TOPIC, LEDGER_ID, ENTRY_ID))
                .thenThrow(new PulsarAdminException("Broker not reachable"));

        committer.commit(Collections.singletonList(request));
        verify(request).signalFailedWithKnownReason(any(FlinkRuntimeException.class));
    }

    @Test
    void commitRetriesOnRetriableTransactionException() throws Exception {
        PulsarCommitter committer = createCommitter();
        injectMocks(committer);

        TxnID txnID = randomTxnID();
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(LEDGER_ID, ENTRY_ID, 0, 0);
        Map<String, BatchMessageIdImpl> messages = Collections.singletonMap(TOPIC, msgId);
        PulsarCommittable committable = new PulsarCommittable(txnID, messages);
        CommitRequest<PulsarCommittable> request = mockCommitRequest(committable, 0);

        @SuppressWarnings("unchecked")
        Message<byte[]> mockMessage = buildUncommittedMessage();
        List<Message<byte[]>> messageList = buildMessageList(mockMessage);

        when(getTopicsMock().getMessagesById(TOPIC, LEDGER_ID, ENTRY_ID))
                .thenReturn(messageList);

        TransactionCoordinatorClientException ex =
                new TransactionCoordinatorClientException("retriable error");
        doThrow(ex).when(coordinatorClient).commit(txnID);

        committer.commit(Collections.singletonList(request));
        verify(request).retryLater();
    }

    @Test
    void commitFailsAfterMaxRetries() throws Exception {
        PulsarCommitter committer = createCommitter();
        injectMocks(committer);

        TxnID txnID = randomTxnID();
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(LEDGER_ID, ENTRY_ID, 0, 0);
        Map<String, BatchMessageIdImpl> messages = Collections.singletonMap(TOPIC, msgId);
        PulsarCommittable committable = new PulsarCommittable(txnID, messages);
        CommitRequest<PulsarCommittable> request = mockCommitRequest(committable, 3);

        @SuppressWarnings("unchecked")
        Message<byte[]> mockMessage = buildUncommittedMessage();
        List<Message<byte[]>> messageList = buildMessageList(mockMessage);

        when(getTopicsMock().getMessagesById(TOPIC, LEDGER_ID, ENTRY_ID))
                .thenReturn(messageList);

        TransactionCoordinatorClientException ex =
                new TransactionCoordinatorClientException("retriable error");
        doThrow(ex).when(coordinatorClient).commit(txnID);

        committer.commit(Collections.singletonList(request));
        verify(request).signalFailedWithKnownReason(any(FlinkRuntimeException.class));
    }

    @Test
    void commitSignalsAlreadyCommittedForTransactionNotFoundAfterRetry() throws Exception {
        PulsarCommitter committer = createCommitter();
        injectMocks(committer);

        TxnID txnID = randomTxnID();
        BatchMessageIdImpl msgId = new BatchMessageIdImpl(LEDGER_ID, ENTRY_ID, 0, 0);
        Map<String, BatchMessageIdImpl> messages = Collections.singletonMap(TOPIC, msgId);
        PulsarCommittable committable = new PulsarCommittable(txnID, messages);
        CommitRequest<PulsarCommittable> request = mockCommitRequest(committable, 1);

        @SuppressWarnings("unchecked")
        Message<byte[]> mockMessage = buildUncommittedMessage();
        List<Message<byte[]>> messageList = buildMessageList(mockMessage);

        when(getTopicsMock().getMessagesById(TOPIC, LEDGER_ID, ENTRY_ID))
                .thenReturn(messageList);

        doThrow(new TransactionNotFoundException("txn not found")).when(coordinatorClient).commit(txnID);

        committer.commit(Collections.singletonList(request));
        verify(request).signalAlreadyCommitted();
    }

    @Test
    void closeWithNullPulsarClientDoesNotThrow() {
        PulsarCommitter committer = createCommitter();
        assertThatCode(committer::close).doesNotThrowAnyException();
    }

    // --- helpers ---

    private PulsarCommitter createCommitter() {
        CommitterInitContext mockContext = mock(CommitterInitContext.class);
        return new PulsarCommitter(sinkConfiguration, mockContext);
    }

    private void injectMocks(PulsarCommitter committer) throws Exception {
        java.lang.reflect.Field pulsarAdminField =
                PulsarCommitter.class.getDeclaredField("pulsarAdmin");
        pulsarAdminField.setAccessible(true);
        pulsarAdminField.set(committer, pulsarAdmin);

        java.lang.reflect.Field coordinatorField =
                PulsarCommitter.class.getDeclaredField("coordinatorClient");
        coordinatorField.setAccessible(true);
        coordinatorField.set(committer, coordinatorClient);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CommitRequest<PulsarCommittable> mockCommitRequest(PulsarCommittable committable) {
        return mockCommitRequest(committable, 0);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private CommitRequest<PulsarCommittable> mockCommitRequest(
            PulsarCommittable committable, int numberOfRetries) {
        CommitRequest<PulsarCommittable> request = mock(CommitRequest.class);
        when(request.getCommittable()).thenReturn(committable);
        when(request.getNumberOfRetries()).thenReturn(numberOfRetries);
        return request;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private org.apache.pulsar.client.admin.Topics getTopicsMock() {
        return (org.apache.pulsar.client.admin.Topics) topicsMock;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Message<byte[]> buildUncommittedMessage() {
        Message<byte[]> mockMessage = mock(Message.class);
        Map<String, String> props = new HashMap<>();
        props.put(TXN_UNCOMMITTED, "true");
        when(mockMessage.getProperties()).thenReturn(props);
        return mockMessage;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private List<Message<byte[]>> buildMessageList(Message<byte[]> message) {
        List<Message<byte[]>> messageList = new ArrayList<>();
        messageList.add(message);
        return messageList;
    }

    private static TxnID randomTxnID() {
        return new TxnID(
                ThreadLocalRandom.current().nextLong(),
                ThreadLocalRandom.current().nextLong());
    }
}
