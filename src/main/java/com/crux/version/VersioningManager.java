package com.crux.version;

import com.crux.persistence.PersistenceManager;
import com.crux.store.Entity;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maintains simple time-travel history for entities.
 */
public class VersioningManager {
    private static final Logger LOGGER = Logger.getLogger(VersioningManager.class.getName());

    private record Version(long timestamp, Map<String, Object> fields, boolean deleted) {
        private Version(long timestamp, Map<String, Object> fields, boolean deleted) {
            this.timestamp = timestamp;
            this.fields = fields == null ? null : deepCopy(fields);
            this.deleted = deleted;
        }
    }

    private final Map<String, List<Version>> history = new HashMap<>();

    public void recordInsert(Entity entity) {
        if (entity == null) {
            LOGGER.severe("recordInsert called with null entity");
            return;
        }
        try {
            addVersion(entity.getId(), new Version(System.currentTimeMillis(), entity.getFields(), false));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to record insert", e);
        }
    }

    public void recordUpdate(String id, Map<String, Object> fields) {
        if (id == null || fields == null) {
            LOGGER.severe("recordUpdate called with null id or fields");
            return;
        }
        try {
            addVersion(id, new Version(System.currentTimeMillis(), fields, false));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to record update for id " + id, e);
        }
    }

    public void recordDelete(String id) {
        if (id == null) {
            LOGGER.severe("recordDelete called with null id");
            return;
        }
        try {
            addVersion(id, new Version(System.currentTimeMillis(), null, true));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to record delete for id " + id, e);
        }
    }

    public Map<String, Object> getAt(String id, long timestamp) {
        if (id == null) {
            LOGGER.warning("getAt called with null id");
            return null;
        }
        try {
            List<Version> versions = history.get(id);
            if (versions == null) {
                return null;
            }
            Version result = null;
            for (Version v : versions) {
                if (v.timestamp <= timestamp) {
                    result = v;
                } else {
                    break;
                }
            }
            if (result == null || result.deleted) {
                return null;
            }
            return deepCopy(result.fields);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get version at time for id " + id, e);
            return null;
        }
    }

    public List<Map<String, Object>> getHistory(String id) {
        if (id == null) {
            LOGGER.warning("getHistory called with null id");
            return Collections.emptyList();
        }
        try {
            List<Version> versions = history.getOrDefault(id, Collections.emptyList());
            List<Map<String, Object>> out = new ArrayList<>();
            for (Version v : versions) {
                Map<String, Object> snapshot = v.fields == null ? new LinkedHashMap<>() : deepCopy(v.fields);
                snapshot.put("_timestamp", v.timestamp);
                snapshot.put("_deleted", v.deleted);
                out.add(snapshot);
            }
            return out;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get history for id " + id, e);
            return Collections.emptyList();
        }
    }

    public Map<String, Map<String, Object>> snapshotAt(long timestamp) {
        Map<String, Map<String, Object>> snapshot = new LinkedHashMap<>();
        for (String id : history.keySet()) {
            Map<String, Object> state = getAt(id, timestamp);
            if (state != null) {
                snapshot.put(id, state);
            }
        }
        return snapshot;
    }

    public void bootstrap(Collection<PersistenceManager.LogEntry> entries) {
        history.clear();
        if (entries == null) {
            return;
        }
        List<PersistenceManager.LogEntry> ordered = new ArrayList<>(entries);
        ordered.sort(Comparator.comparingLong(PersistenceManager.LogEntry::timestamp));
        for (PersistenceManager.LogEntry entry : ordered) {
            PersistenceManager.Operation op = entry.operation();
            if (op == null || entry.id() == null) {
                continue;
            }
            switch (op) {
                case INSERT, UPDATE -> addVersion(entry.id(), new Version(entry.timestamp(), entry.fields(), false));
                case DELETE -> addVersion(entry.id(), new Version(entry.timestamp(), null, true));
            }
        }
    }

    private void addVersion(String id, Version version) {
        if (id == null || version == null) {
            return;
        }
        List<Version> versions = history.computeIfAbsent(id, k -> new ArrayList<>());
        if (!versions.isEmpty() && versions.get(versions.size() - 1).timestamp <= version.timestamp) {
            versions.add(version);
        } else {
            versions.add(version);
            versions.sort(Comparator.comparingLong(Version::timestamp));
        }
    }

    private static Map<String, Object> deepCopy(Map<String, Object> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (var entry : source.entrySet()) {
            copy.put(entry.getKey(), deepCopyValue(entry.getValue()));
        }
        return copy;
    }

    private static Object deepCopyValue(Object value) {
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
}
