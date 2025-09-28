package com.crux;

import com.crux.query.FilterParser;
import com.crux.store.DocumentStore;
import com.crux.store.Entity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FilterParserTest {
    @Test
    public void testNumericComparison(@TempDir Path tempDir) {
        DocumentStore store = new DocumentStore(tempDir);
        store.insert(new Entity("1", Map.of("age", 30)));
        store.insert(new Entity("2", Map.of("age", 25)));
        FilterParser parser = new FilterParser();
        var expr = parser.parse("age >= 30");
        var res = store.query(expr);
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).getId());
    }

    @Test
    public void testBooleanComparison(@TempDir Path tempDir) {
        DocumentStore store = new DocumentStore(tempDir);
        store.insert(new Entity("1", Map.of("flag", true)));
        store.insert(new Entity("2", Map.of("flag", false)));
        FilterParser parser = new FilterParser();
        var expr = parser.parse("flag == true");
        var res = store.query(expr);
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).getId());
    }

    @Test
    public void testStringComparison(@TempDir Path tempDir) {
        DocumentStore store = new DocumentStore(tempDir);
        store.insert(new Entity("1", Map.of("name", "alice")));
        store.insert(new Entity("2", Map.of("name", "bob")));
        FilterParser parser = new FilterParser();
        var expr = parser.parse("name == \"alice\"");
        var res = store.query(expr);
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).getId());
    }

    @Test
    public void testComplexExpression(@TempDir Path tempDir) {
        DocumentStore store = new DocumentStore(tempDir);
        store.insert(new Entity("1", Map.of("feature", Map.of("tag", 4), "id", 150)));
        store.insert(new Entity("2", Map.of("feature", Map.of("tag", 5), "id", 187)));
        FilterParser parser = new FilterParser();
        var expr = parser.parse("id = (25 * &feature.tag/2) * 3");
        var res = store.query(expr);
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).getId());
    }

    @Test
    public void testContainsAndLike(@TempDir Path tempDir) {
        DocumentStore store = new DocumentStore(tempDir);
        store.insert(new Entity("1", Map.of("title", "Data Engineering")));
        store.insert(new Entity("2", Map.of("title", "Data Science")));
        FilterParser parser = new FilterParser();
        var contains = parser.parse("title contains \"engine\"");
        var resContains = store.query(contains);
        assertEquals(1, resContains.size());
        assertEquals("1", resContains.get(0).getId());

        var like = parser.parse("title like \"Data%Science\"");
        var resLike = store.query(like);
        assertEquals(1, resLike.size());
        assertEquals("2", resLike.get(0).getId());
    }

    @Test
    public void testNotOperator(@TempDir Path tempDir) {
        DocumentStore store = new DocumentStore(tempDir);
        store.insert(new Entity("1", Map.of("age", 30)));
        store.insert(new Entity("2", Map.of("age", 20)));
        FilterParser parser = new FilterParser();
        var expr = parser.parse("not age < 25");
        var res = store.query(expr);
        assertEquals(1, res.size());
        assertEquals("1", res.get(0).getId());
    }

    @Test
    public void testHyphenatedIdentifierWithoutQuotes(@TempDir Path tempDir) {
        DocumentStore store = new DocumentStore(tempDir);
        String uuid = "e1395e90-4773-4089-a1bb-5362f2ff79da";
        store.insert(new Entity(uuid, Map.of("id", uuid)));
        FilterParser parser = new FilterParser();
        var expr = parser.parse("id == " + uuid);
        var res = store.query(expr);
        assertEquals(1, res.size());
        assertEquals(uuid, res.get(0).getId());
    }
}
