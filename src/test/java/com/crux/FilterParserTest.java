package com.crux;

import com.crux.query.FilterParser;
import com.crux.store.DocumentStore;
import com.crux.store.Entity;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FilterParserTest {
    @Test
    public void testNumericComparison() {
        DocumentStore store = new DocumentStore();
        store.insert(new Entity("1", Map.of("age", 30)));
        store.insert(new Entity("2", Map.of("age", 25)));
        FilterParser parser = new FilterParser();
        var expr = parser.parse("age >= 30");
        var res = store.query(expr);
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).getId());
    }
}
