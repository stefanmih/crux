package com.crux.cli;

import com.crux.store.DocumentStore;
import com.crux.store.Entity;
import com.google.gson.Gson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class CommandParserTest {

    private static class CliHarness {
        final CommandLine cli;
        final DocumentStore store;

        CliHarness(CommandLine cli, DocumentStore store) {
            this.cli = cli;
            this.store = store;
        }
    }

    private CliHarness createHarness(Path dir) throws Exception {
        CommandLine cli = new CommandLine();
        DocumentStore store = new DocumentStore(dir);
        Field storeField = CommandLine.class.getDeclaredField("store");
        storeField.setAccessible(true);
        storeField.set(cli, store);
        cli.sets.clear();
        cli.sets.put("all", new HashSet<>(store.getAllIds()));
        return new CliHarness(cli, store);
    }

    private void insertEntity(CliHarness harness, String id, Map<String, Object> fields) {
        Map<String, Object> copy = new HashMap<>(fields);
        copy.putIfAbsent("id", id);
        harness.store.insert(new Entity(id, copy));
        harness.cli.sets.get("all").add(id);
    }

    private CommandParser.Command parse(String line) {
        return new CommandParser().parse(line);
    }

    private String executeAndCapture(CommandParser.Command command, CommandLine cli) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(baos));
        try {
            command.execute(cli);
        } finally {
            System.setOut(original);
        }
        return baos.toString().trim();
    }

    @Test
    public void testAddEntityParsesNestedJsonAndVector(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        String cmd = "add entity {\"id\":\"1\",\"name\":\"Alice\",\"nested\":{\"value\":5}} vector [1 2 3]";
        executeAndCapture(parse(cmd), harness.cli);
        Entity inserted = harness.store.get("1");
        assertNotNull(inserted);
        Map<String, Object> nested = (Map<String, Object>) inserted.get("nested");
        assertEquals(5.0, ((Number) nested.get("value")).doubleValue());
        List<Double> vector = (List<Double>) inserted.get("vector");
        assertEquals(List.of(1.0, 2.0, 3.0), vector);
        assertTrue(harness.cli.sets.get("all").contains("1"));
    }

    @Test
    public void testDeleteEntityRemovesFromStoreAndSets(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", Map.of("name", "Alice"));
        executeAndCapture(parse("delete entity 1"), harness.cli);
        assertNull(harness.store.get("1"));
        assertFalse(harness.cli.sets.get("all").contains("1"));
    }

    @Test
    public void testUpdateEntitiesParsesFilterAndJson(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", new HashMap<>(Map.of("name", "Alice", "nested", Map.of("value", 5))));
        String cmd = "update entities where id == \"1\" set {\"nested\":{\"value\":10},\"status\":\"active\"}";
        executeAndCapture(parse(cmd), harness.cli);
        Entity updated = harness.store.get("1");
        Map<String, Object> nested = (Map<String, Object>) updated.get("nested");
        assertEquals(10.0, ((Number) nested.get("value")).doubleValue());
        assertEquals("active", updated.get("status"));
    }

    @Test
    public void testGetEntitiesUsingFilterOutputsJson(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", Map.of("name", "Alice"));
        insertEntity(harness, "2", Map.of("name", "Bob"));
        String output = executeAndCapture(parse("get entities using filter name == \"Alice\""), harness.cli);
        List<Map<String, Object>> result = new Gson().fromJson(output, List.class);
        assertEquals(1, result.size());
        assertEquals("1", result.get(0).get("id"));
    }

    @Test
    public void testGetFieldOutputsNestedValue(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", Map.of("nested", Map.of("value", 42)));
        String output = executeAndCapture(parse("get field nested.value from 1"), harness.cli);
        Number value = new Gson().fromJson(output, Number.class);
        assertEquals(42.0, value.doubleValue(), 1e-9);
    }

    @Test
    public void testGetSomeWithExplicitCount(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", Map.of("name", "A"));
        insertEntity(harness, "2", Map.of("name", "B"));
        insertEntity(harness, "3", Map.of("name", "C"));
        String output = executeAndCapture(parse("get some [2]"), harness.cli);
        List<?> result = new Gson().fromJson(output, List.class);
        assertEquals(2, result.size());
    }

    @Test
    public void testGetSomeDefaultsToAllWhenLessThanFive(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", Map.of("name", "A"));
        insertEntity(harness, "2", Map.of("name", "B"));
        insertEntity(harness, "3", Map.of("name", "C"));
        String output = executeAndCapture(parse("get some"), harness.cli);
        List<?> result = new Gson().fromJson(output, List.class);
        assertEquals(3, result.size());
    }

    @Test
    public void testGenerateCreatesEntities(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        executeAndCapture(parse("generate 4"), harness.cli);
        assertEquals(4, harness.store.findAll().size());
        assertEquals(4, harness.cli.sets.get("all").size());
    }

    @Test
    public void testFindSimilarOutputsTopMatches(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "base", Map.of("vector", List.of(1.0, 0.0, 0.0), "tag", "base"));
        insertEntity(harness, "close", Map.of("vector", List.of(1.0, 0.1, 0.0), "tag", "close"));
        insertEntity(harness, "far", Map.of("vector", List.of(0.0, 1.0, 0.0), "tag", "far"));
        String output = executeAndCapture(parse("find similar base 1"), harness.cli);
        List<Map<String, Object>> result = new Gson().fromJson(output, List.class);
        assertEquals(1, result.size());
        assertEquals("close", result.get(0).get("tag"));
    }

    @Test
    public void testCreateTransformFunctionParsesExpressions(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        String command = "create transform function { &value * 2 -> double.value; &name -> name }";
        executeAndCapture(parse(command), harness.cli);
        assertEquals(2, harness.cli.transformFunction.size());
        Entity entity = new Entity("e", Map.of("value", 5, "name", "Alice"));
        assertEquals(10.0, ((Number) harness.cli.transformFunction.get("double.value").eval(entity)).doubleValue());
        assertEquals("Alice", harness.cli.transformFunction.get("name").eval(entity));
    }

    @Test
    public void testApplyTransformFunctionCreatesDerivedSet(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "source", Map.of("value", 10, "name", "Bob"));
        executeAndCapture(parse("create transform function { &value * 2 -> double.value; &name -> metadata.owner }"), harness.cli);
        Set<String> before = new HashSet<>(harness.store.getAllIds());
        executeAndCapture(parse("apply transform function from set all to derived"), harness.cli);
        assertTrue(harness.cli.sets.containsKey("derived"));
        assertEquals(1, harness.cli.sets.get("derived").size());
        Set<String> after = new HashSet<>(harness.store.getAllIds());
        after.removeAll(before);
        assertEquals(1, after.size());
        String newId = after.iterator().next();
        Entity transformed = harness.store.get(newId);
        Map<String, Object> doubleMap = (Map<String, Object>) transformed.get("double");
        assertEquals(20.0, ((Number) doubleMap.get("value")).doubleValue());
        Map<String, Object> metadata = (Map<String, Object>) transformed.get("metadata");
        assertEquals("Bob", metadata.get("owner"));
    }

    @Test
    public void testShowHistoryIncludesUpdates(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", new HashMap<>(Map.of("name", "Alice")));
        executeAndCapture(parse("update entities where id == \"1\" set {\"name\":\"Alicia\"}"), harness.cli);
        String output = executeAndCapture(parse("show history 1"), harness.cli);
        List<Map<String, Object>> history = new Gson().fromJson(output, List.class);
        assertTrue(history.size() >= 2);
        Map<String, Object> last = history.get(history.size() - 1);
        assertEquals("Alicia", last.get("name"));
    }

    @Test
    public void testPersistSnapshotCreatesFile(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        insertEntity(harness, "1", Map.of("name", "Alice"));
        executeAndCapture(parse("persist snapshot"), harness.cli);
        assertTrue(Files.exists(tempDir.resolve("snapshot.json")));
        assertFalse(Files.exists(tempDir.resolve("wal.log")));
    }

    @Test
    public void testHelpCommandPrintsUsage(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        String output = executeAndCapture(parse("help"), harness.cli);
        assertTrue(output.contains("Available commands:"));
        assertTrue(output.contains("add entity"));
    }

    @Test
    public void testUnknownCommandThrows() {
        assertThrows(RuntimeException.class, () -> parse("foobar"));
    }

    @Test
    public void testMalformedGetCommandThrows(@TempDir Path tempDir) throws Exception {
        CliHarness harness = createHarness(tempDir);
        assertThrows(RuntimeException.class, () -> parse("get entities filter"));
        assertThrows(RuntimeException.class, () -> parse("get field name 1").execute(harness.cli));
    }
}
