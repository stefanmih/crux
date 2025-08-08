package rs.clustiqdb.version;

import rs.clustiqdb.store.Entity;

import java.util.*;

/**
 * Maintains simple time-travel history for entities.
 */
public class VersioningManager {
    private static class Version {
        final long timestamp;
        final Map<String, Object> fields;
        Version(long timestamp, Map<String, Object> fields) {
            this.timestamp = timestamp;
            this.fields = new HashMap<>(fields);
        }
    }

    private final Map<String, List<Version>> history = new HashMap<>();

    public void recordInsert(Entity entity) {
        history.computeIfAbsent(entity.getId(), k -> new ArrayList<>())
                .add(new Version(System.currentTimeMillis(), entity.getFields()));
    }

    public void recordUpdate(String id, Map<String, Object> fields) {
        history.computeIfAbsent(id, k -> new ArrayList<>())
                .add(new Version(System.currentTimeMillis(), fields));
    }

    public void recordDelete(String id) {
        history.computeIfAbsent(id, k -> new ArrayList<>())
                .add(new Version(System.currentTimeMillis(), Collections.emptyMap()));
    }

    public Map<String, Object> getAt(String id, long timestamp) {
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
    }
}
