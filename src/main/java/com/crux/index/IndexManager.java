package com.crux.index;

import com.crux.store.Entity;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maintains simple in-memory indexes for entity fields.
 */
public class IndexManager {
    private static final Logger LOGGER = Logger.getLogger(IndexManager.class.getName());

    private final Map<String, NavigableMap<Comparable, Set<String>>> indexes = new HashMap<>();

    public void index(Entity entity) {
        if (entity == null) {
            LOGGER.severe("Attempted to index null entity");
            return;
        }
        try {
            String id = entity.getId();
            for (Map.Entry<String, Object> e : entity.getFields().entrySet()) {
                Object value = e.getValue();
                if (value instanceof Comparable) {
                    NavigableMap<Comparable, Set<String>> map = indexes.computeIfAbsent(e.getKey(), k -> new TreeMap<>());
                    Set<String> ids = map.computeIfAbsent((Comparable) value, v -> new HashSet<>());
                    ids.add(id);
                }
            }
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Failed to index entity", ex);
        }
    }

    public void remove(Entity entity) {
        if (entity == null) {
            LOGGER.severe("Attempted to remove null entity from index");
            return;
        }
        try {
            String id = entity.getId();
            for (Map.Entry<String, Object> e : entity.getFields().entrySet()) {
                Object value = e.getValue();
                if (value instanceof Comparable) {
                    NavigableMap<Comparable, Set<String>> map = indexes.get(e.getKey());
                    if (map != null) {
                        Set<String> ids = map.get((Comparable) value);
                        if (ids != null) {
                            ids.remove(id);
                            if (ids.isEmpty()) {
                                map.remove((Comparable) value);
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Failed to remove entity from index", ex);
        }
    }

    public Set<String> searchEquals(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchEquals called with null field or value");
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return new HashSet<>(map.getOrDefault(value, Collections.emptySet()));
    }

    public Set<String> searchGreaterThan(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchGreaterThan called with null field or value");
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.tailMap(value, false).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    public Set<String> searchLessThan(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchLessThan called with null field or value");
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.headMap(value, false).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }
}
