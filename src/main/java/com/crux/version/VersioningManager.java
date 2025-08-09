package com.crux.version;

import com.crux.store.Entity;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maintains simple time-travel history for entities.
 */
public class VersioningManager {
    private static final Logger LOGGER = Logger.getLogger(VersioningManager.class.getName());

    private record Version(long timestamp, Map<String, Object> fields) {
            private Version(long timestamp, Map<String, Object> fields) {
                this.timestamp = timestamp;
                this.fields = new HashMap<>(fields);
            }
        }

    private final Map<String, List<Version>> history = new HashMap<>();

    public void recordInsert(Entity entity) {
        if (entity == null) {
            LOGGER.severe("recordInsert called with null entity");
            return;
        }
        try {
            history.computeIfAbsent(entity.getId(), k -> new ArrayList<>())
                    .add(new Version(System.currentTimeMillis(), entity.getFields()));
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
            history.computeIfAbsent(id, k -> new ArrayList<>())
                    .add(new Version(System.currentTimeMillis(), fields));
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
            history.computeIfAbsent(id, k -> new ArrayList<>())
                    .add(new Version(System.currentTimeMillis(), Collections.emptyMap()));
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
            return result == null ? null : new HashMap<>(result.fields);
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
                out.add(new HashMap<>(v.fields));
            }
            return out;
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get history for id " + id, e);
            return Collections.emptyList();
        }
    }
}
