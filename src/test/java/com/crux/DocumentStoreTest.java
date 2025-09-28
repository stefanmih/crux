package com.crux;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.crux.store.DocumentStore;
import com.crux.store.Entity;
import com.crux.query.QueryExpression;
import com.crux.pipeline.Pipeline;

import java.util.*;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentStoreTest {
    @Test
    public void testStoreQueryPipelineAndVersioning(@TempDir Path tempDir) throws InterruptedException {
        DocumentStore store = new DocumentStore(tempDir);
        store.insert(new Entity("1", Map.of(
                "age", 30,
                "name", "Alice",
                "address", Map.of("city", "Belgrade"))));
        store.insert(new Entity("2", Map.of("age", 25, "name", "Bob")));
        store.insert(new Entity("3", Map.of("age", 40, "name", "Carol")));

        List<Entity> olderThan29 = store.query(QueryExpression.field("age", QueryExpression.Operator.GT, 29));
        assertEquals(2, olderThan29.size());

        List<Entity> belgrade = store.query(QueryExpression.field("address.city", QueryExpression.Operator.EQ, "Belgrade"));
        assertEquals(1, belgrade.size());

        List<Entity> bob = store.query(QueryExpression.and(
                QueryExpression.field("age", QueryExpression.Operator.LTE, 35),
                QueryExpression.contains("name", "O")));
        assertEquals(1, bob.size());
        assertEquals("Bob", bob.get(0).get("name"));

        Pipeline<Entity> pipeline = new Pipeline<>(store.findAll());
        double averageAge = pipeline.average(e -> ((Number) e.get("age")).doubleValue());
        assertEquals((30 + 25 + 40) / 3.0, averageAge, 1e-6);

        long timestamp = System.currentTimeMillis();
        Thread.sleep(5); // ensure timestamp difference
        store.updatePartial("1", Map.of("age", 31, "nickname", "Ace"));

        Entity previous = store.getAt("1", timestamp);
        assertNotNull(previous);
        assertEquals(30.0, ((Number) previous.get("age")).doubleValue());
        assertNull(previous.get("nickname"));

        List<Entity> snapshot = store.snapshotAt(timestamp);
        assertEquals(3, snapshot.size());
        Optional<Entity> originalAlice = snapshot.stream().filter(e -> e.getId().equals("1")).findFirst();
        assertTrue(originalAlice.isPresent());
        assertEquals(30.0, ((Number) originalAlice.get().get("age")).doubleValue());

        store.delete("2");
        List<Map<String, Object>> history = store.getHistory("2");
        assertFalse(history.isEmpty());
        assertEquals(Boolean.TRUE, history.get(history.size() - 1).get("_deleted"));

        store.saveSnapshot();
        DocumentStore reloaded = new DocumentStore(tempDir);
        assertNull(reloaded.get("2"));
        assertEquals(2, reloaded.findAll().size());

        long reloadTimestamp = System.currentTimeMillis();
        Thread.sleep(5);
        reloaded.update("1", Map.of("age", 33, "name", "Alice"));
        DocumentStore reloadedAgain = new DocumentStore(tempDir);
        assertEquals(33.0, ((Number) reloadedAgain.get("1").get("age")).doubleValue());
        assertNotNull(reloadedAgain.getAt("1", reloadTimestamp));
    }
}
