package com.crux.persistence;

import com.crux.store.Entity;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.util.*;

/**
 * Handles persistence of the in-memory store using snapshot and WAL files.
 */
public class PersistenceManager {
    private static final Type SNAPSHOT_TYPE = new TypeToken<Map<String, Map<String, Object>>>() {}.getType();

    private final Path baseDirectory;
    private final Path snapshotPath;
    private final Path walPath;
    private final Gson gson = new GsonBuilder().create();

    public PersistenceManager(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
        this.snapshotPath = baseDirectory.resolve("snapshot.json");
        this.walPath = baseDirectory.resolve("wal.log");
        try {
            Files.createDirectories(baseDirectory);
        } catch (IOException e) {
            throw new RuntimeException("Unable to create persistence directory", e);
        }
    }

    public LoadedState load() {
        Map<String, Map<String, Object>> data = new LinkedHashMap<>();
        List<LogEntry> history = new ArrayList<>();
        long snapshotTimestamp = 0L;
        try {
            if (Files.exists(snapshotPath)) {
                snapshotTimestamp = Files.getLastModifiedTime(snapshotPath).toMillis();
                try (Reader reader = Files.newBufferedReader(snapshotPath, StandardCharsets.UTF_8)) {
                    Map<String, Map<String, Object>> snapshot = gson.fromJson(reader, SNAPSHOT_TYPE);
                    if (snapshot != null) {
                        for (var entry : snapshot.entrySet()) {
                            Map<String, Object> copy = deepCopy(entry.getValue());
                            data.put(entry.getKey(), copy);
                            history.add(new LogEntry(Operation.INSERT, entry.getKey(), deepCopy(copy), snapshotTimestamp));
                        }
                    }
                }
            }
            if (Files.exists(walPath)) {
                try (BufferedReader reader = Files.newBufferedReader(walPath, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) {
                            continue;
                        }
                        WalEntry entry = gson.fromJson(line, WalEntry.class);
                        if (entry == null || entry.id == null || entry.operation == null) {
                            continue;
                        }
                        Map<String, Object> copy = entry.fields == null ? null : deepCopy(entry.fields);
                        apply(data, entry.operation, entry.id, copy);
                        history.add(new LogEntry(entry.operation, entry.id, copy, entry.timestamp));
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load persisted state", e);
        }
        history.sort(Comparator.comparingLong(LogEntry::timestamp));
        return new LoadedState(data, history);
    }

    public void appendInsert(Entity entity) throws IOException {
        Map<String, Object> copy = deepCopy(entity.getFields());
        append(new LogEntry(Operation.INSERT, entity.getId(), copy, System.currentTimeMillis()));
    }

    public void appendUpdate(String id, Map<String, Object> fields) throws IOException {
        append(new LogEntry(Operation.UPDATE, id, deepCopy(fields), System.currentTimeMillis()));
    }

    public void appendDelete(String id) throws IOException {
        append(new LogEntry(Operation.DELETE, id, null, System.currentTimeMillis()));
    }

    public void saveSnapshot(Collection<Entity> entities) throws IOException {
        Map<String, Map<String, Object>> snapshot = new LinkedHashMap<>();
        for (Entity entity : entities) {
            snapshot.put(entity.getId(), deepCopy(entity.getFields()));
        }
        Files.createDirectories(baseDirectory);
        Path tmp = baseDirectory.resolve("snapshot.tmp");
        try (Writer writer = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            gson.toJson(snapshot, writer);
        }
        try {
            Files.move(tmp, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ex) {
            Files.move(tmp, snapshotPath, StandardCopyOption.REPLACE_EXISTING);
        }
        Files.deleteIfExists(walPath);
        try {
            Files.setLastModifiedTime(snapshotPath, FileTime.fromMillis(System.currentTimeMillis()));
        } catch (IOException ignored) {
        }
    }

    private void append(LogEntry entry) throws IOException {
        WalEntry walEntry = new WalEntry(entry.operation(), entry.id(), entry.fields() == null ? null : deepCopy(entry.fields()), entry.timestamp());
        String json = gson.toJson(walEntry);
        Files.writeString(walPath, json + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private void apply(Map<String, Map<String, Object>> data, Operation op, String id, Map<String, Object> fields) {
        switch (op) {
            case INSERT, UPDATE -> {
                if (fields != null) {
                    data.put(id, deepCopy(fields));
                }
            }
            case DELETE -> data.remove(id);
        }
    }

    private Map<String, Object> deepCopy(Map<String, Object> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (var entry : source.entrySet()) {
            copy.put(entry.getKey(), deepCopyValue(entry.getValue()));
        }
        return copy;
    }

    private Object deepCopyValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> nested = new LinkedHashMap<>();
            for (var entry : map.entrySet()) {
                nested.put(String.valueOf(entry.getKey()), deepCopyValue(entry.getValue()));
            }
            return nested;
        }
        if (value instanceof List<?> list) {
            List<Object> nested = new ArrayList<>();
            for (Object o : list) {
                nested.add(deepCopyValue(o));
            }
            return nested;
        }
        return value;
    }

    public record LoadedState(Map<String, Map<String, Object>> data, List<LogEntry> history) {}

    public record LogEntry(Operation operation, String id, Map<String, Object> fields, long timestamp) {}

    public enum Operation { INSERT, UPDATE, DELETE }

    private static final class WalEntry {
        Operation operation;
        String id;
        Map<String, Object> fields;
        long timestamp;

        WalEntry() {
        }

        WalEntry(Operation operation, String id, Map<String, Object> fields, long timestamp) {
            this.operation = operation;
            this.id = id;
            this.fields = fields;
            this.timestamp = timestamp;
        }
    }
}

