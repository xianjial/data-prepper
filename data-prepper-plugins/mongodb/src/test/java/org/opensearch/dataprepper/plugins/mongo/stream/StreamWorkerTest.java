package org.opensearch.dataprepper.plugins.mongo.stream;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import io.micrometer.core.instrument.Counter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.mongo.buffer.RecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.client.MongoDBConnection;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.FAILURE_ITEM_COUNTER_NAME;
import static org.opensearch.dataprepper.plugins.mongo.stream.StreamWorker.SUCCESS_ITEM_COUNTER_NAME;

@ExtendWith(MockitoExtension.class)
public class StreamWorkerTest {
    @Mock
    private RecordBufferWriter mockRecordBufferWriter;
    @Mock
    private AcknowledgementSetManager mockAcknowledgementSetManager;
    @Mock
    private MongoDBSourceConfig mockSourceConfig;
    @Mock
    private DataStreamPartitionCheckpoint mockPartitionCheckpoint;
    @Mock
    private StreamPartition streamPartition;
    @Mock
    private StreamProgressState streamProgressState;
    @Mock
    private Counter successItemsCounter;
    @Mock
    private Counter failureItemsCounter;

    @Mock
    private PluginMetrics mockPluginMetrics;

    private StreamWorker streamWorker;

    @BeforeEach
    public void setup() {
        when(mockPluginMetrics.counter(SUCCESS_ITEM_COUNTER_NAME)).thenReturn(successItemsCounter);
        when(mockPluginMetrics.counter(FAILURE_ITEM_COUNTER_NAME)).thenReturn(failureItemsCounter);
        streamWorker = new StreamWorker(mockRecordBufferWriter, mockAcknowledgementSetManager,
                mockSourceConfig, mockPartitionCheckpoint, mockPluginMetrics, 2);
    }

    @Test
    void test_processStream_invalidCollection() {
        when(streamPartition.getCollection()).thenReturn(UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> streamWorker.processStream(streamPartition));
    }

    @Test
    void test_processStream_success() {
        when(streamProgressState.shouldWaitForExport()).thenReturn(false);
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamPartition.getCollection()).thenReturn("database.collection");
        MongoClient mongoClient = mock(MongoClient.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoCollection col = mock(MongoCollection.class);
        ChangeStreamIterable changeStreamIterable = mock(ChangeStreamIterable.class);
        MongoCursor cursor = mock(MongoCursor.class);
        lenient().when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        lenient().when(mongoDatabase.getCollection(anyString())).thenReturn(col);
        lenient().when(col.watch()).thenReturn(changeStreamIterable);
        lenient().when(changeStreamIterable.fullDocument(FullDocument.UPDATE_LOOKUP)).thenReturn(changeStreamIterable);
        lenient().when(changeStreamIterable.iterator()).thenReturn(cursor);
        lenient().when(cursor.hasNext()).thenReturn(true, true, false);
        ChangeStreamDocument streamDoc1  = mock(ChangeStreamDocument.class);
        ChangeStreamDocument streamDoc2  = mock(ChangeStreamDocument.class);
        Document doc1 = mock(Document.class);
        Document doc2 =  mock(Document.class);
        BsonDocument bsonDoc1 = new BsonDocument("resumeToken1", new BsonInt32(123));
        BsonDocument bsonDoc2 = new BsonDocument("resumeToken2", new BsonInt32(234));
        when(streamDoc1.getResumeToken()).thenReturn(bsonDoc1);
        when(streamDoc2.getResumeToken()).thenReturn(bsonDoc2);
        lenient().when(cursor.next())
                .thenReturn(streamDoc1)
                .thenReturn(streamDoc2);
        when(streamDoc1.getFullDocument()).thenReturn(doc1);
        when(streamDoc2.getFullDocument()).thenReturn(doc2);

        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenReturn(mongoClient);
            streamWorker.processStream(streamPartition);
        }
        verify(mongoClient, times(1)).close();
        verify(mongoDatabase).getCollection(eq("collection"));
        verify(mockRecordBufferWriter).writeToBuffer(eq(null), any());
        verify(successItemsCounter, times(1)).increment();
        verify(failureItemsCounter, never()).increment();
    }


    @Test
    void test_processStream_mongoClientFailure() {
        when(streamPartition.getProgressState()).thenReturn(Optional.of(streamProgressState));
        when(streamPartition.getCollection()).thenReturn("database.collection");
        MongoClient mongoClient = mock(MongoClient.class);
        try (MockedStatic<MongoDBConnection> mongoDBConnectionMockedStatic = mockStatic(MongoDBConnection.class)) {
            mongoDBConnectionMockedStatic.when(() -> MongoDBConnection.getMongoClient(any(MongoDBSourceConfig.class)))
                    .thenThrow(RuntimeException.class);
            assertThrows(RuntimeException.class, () -> streamWorker.processStream(streamPartition));
        }
        verifyNoInteractions(mongoClient);
        verifyNoInteractions(mockRecordBufferWriter);
        verifyNoInteractions(successItemsCounter);
        verifyNoInteractions(failureItemsCounter);
    }
}