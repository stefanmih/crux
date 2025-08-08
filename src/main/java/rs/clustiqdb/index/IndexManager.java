package rs.clustiqdb.index;

import rs.clustiqdb.store.Entity;

import java.util.*;

/**
 * Maintains simple in-memory indexes for entity fields.
 */
public class IndexManager {
    private final Map<String, NavigableMap<Comparable, Set<String>>> indexes = new HashMap<>();

    public void index(Entity entity) {
        String id = entity.getId();
        for (Map.Entry<String, Object> e : entity.getFields().entrySet()) {
            Object value = e.getValue();
            if (value instanceof Comparable) {
                NavigableMap<Comparable, Set<String>> map = indexes.computeIfAbsent(e.getKey(), k -> new TreeMap<>());
                Set<String> ids = map.computeIfAbsent((Comparable) value, v -> new HashSet<>());
                ids.add(id);
            }
        }
    }

    public void remove(Entity entity) {
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
    }

    public Set<String> searchEquals(String field, Comparable value) {
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return new HashSet<>(map.getOrDefault(value, Collections.emptySet()));
    }

    public Set<String> searchGreaterThan(String field, Comparable value) {
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.tailMap(value, false).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    public Set<String> searchLessThan(String field, Comparable value) {
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.headMap(value, false).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }
}
