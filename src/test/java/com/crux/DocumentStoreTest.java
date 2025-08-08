package com.crux;

import org.junit.jupiter.api.Test;
import com.crux.store.DocumentStore;
import com.crux.store.Entity;
import com.crux.query.QueryExpression;
import com.crux.pipeline.Pipeline;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DocumentStoreTest {
    @Test
    public void testStoreQueryPipelineAndVersioning() throws InterruptedException {
        DocumentStore store = new DocumentStore();
        store.insert(new Entity("1", Map.of("age", 30, "name", "Alice")));
        store.insert(new Entity("2", Map.of("age", 25, "name", "Bob")));
        store.insert(new Entity("3", Map.of("age", 40, "name", "Carol")));

        List<Entity> olderThan29 = store.query(QueryExpression.field("age", QueryExpression.Operator.GT, 29));
        assertEquals(2, olderThan29.size());

        List<Entity> bob = store.query(QueryExpression.and(
                QueryExpression.field("age", QueryExpression.Operator.LT, 35),
                QueryExpression.contains("name", "o")));
        assertEquals(1, bob.size());
        assertEquals("Bob", bob.get(0).get("name"));

        Pipeline<Entity> pipeline = new Pipeline<>(store.findAll());
        Map<Object, List<Entity>> grouped = pipeline.groupBy(e -> e.get("age"));
        assertEquals(3, grouped.size());

        long timestamp = System.currentTimeMillis();
        Thread.sleep(1); // ensure timestamp difference
        store.update("1", Map.of("age", 31, "name", "Alice"));
        Entity previous = store.getAt("1", timestamp);
        assertNotNull(previous);
        assertEquals(30, previous.get("age"));
    }
}
