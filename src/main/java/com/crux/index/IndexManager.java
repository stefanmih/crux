package com.crux.index;

import com.crux.store.Entity;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Maintains simple in-memory indexes for entity fields.
 */
public class IndexManager {
    private static final Logger LOGGER = Logger.getLogger(IndexManager.class.getName());

    private final Map<String, NavigableMap<Comparable, Set<String>>> indexes = new HashMap<>();
    private final Map<String, Map<String, String>> textValues = new HashMap<>();

    public void index(Entity entity) {
        if (entity == null) {
            LOGGER.severe("Attempted to index null entity");
            return;
        }
        try {
            String id = entity.getId();
            entity.getFields().forEach((key, value) -> traverse(key, value, (path, val) -> addValue(path, val, id)));
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
            entity.getFields().forEach((key, value) -> traverse(key, value, (path, val) -> removeValue(path, val, id)));
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Failed to remove entity from index", ex);
        }
    }

    public Set<String> searchEquals(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchEquals called with null field or value");
            return Collections.emptySet();
        }
        Comparable<?> normalized = normalizeComparable(value);
        if (normalized == null) {
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return new HashSet<>(map.getOrDefault(normalized, Collections.emptySet()));
    }

    public Set<String> searchGreaterThan(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchGreaterThan called with null field or value");
            return Collections.emptySet();
        }
        Comparable<?> normalized = normalizeComparable(value);
        if (normalized == null) {
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.tailMap(normalized, false).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    public Set<String> searchLessThan(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchLessThan called with null field or value");
            return Collections.emptySet();
        }
        Comparable<?> normalized = normalizeComparable(value);
        if (normalized == null) {
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.headMap(normalized, false).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    public Set<String> searchGreaterOrEquals(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchGreaterOrEquals called with null field or value");
            return Collections.emptySet();
        }
        Comparable<?> normalized = normalizeComparable(value);
        if (normalized == null) {
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.tailMap(normalized, true).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    public Set<String> searchLessOrEquals(String field, Comparable value) {
        if (field == null || value == null) {
            LOGGER.warning("searchLessOrEquals called with null field or value");
            return Collections.emptySet();
        }
        Comparable<?> normalized = normalizeComparable(value);
        if (normalized == null) {
            return Collections.emptySet();
        }
        NavigableMap<Comparable, Set<String>> map = indexes.get(field);
        if (map == null) {
            return Collections.emptySet();
        }
        return map.headMap(normalized, true).values().stream()
                .collect(HashSet::new, Set::addAll, Set::addAll);
    }

    public Set<String> searchContains(String field, String substring) {
        if (field == null || substring == null) {
            LOGGER.warning("searchContains called with null arguments");
            return Collections.emptySet();
        }
        Map<String, String> values = textValues.get(field);
        if (values == null) {
            return Collections.emptySet();
        }
        String needle = substring.toLowerCase(Locale.ROOT);
        Set<String> result = new HashSet<>();
        for (var entry : values.entrySet()) {
            if (entry.getValue().contains(needle)) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    public Set<String> searchLike(String field, String pattern) {
        if (field == null || pattern == null) {
            LOGGER.warning("searchLike called with null arguments");
            return Collections.emptySet();
        }
        Map<String, String> values = textValues.get(field);
        if (values == null) {
            return Collections.emptySet();
        }
        Pattern regex = Pattern.compile(convertLikePattern(pattern.toLowerCase(Locale.ROOT)));
        Set<String> result = new HashSet<>();
        for (var entry : values.entrySet()) {
            if (regex.matcher(entry.getValue()).matches()) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    private void addValue(String path, Object value, String id) {
        if (value == null || path == null) {
            return;
        }
        Comparable<?> comparable = normalizeComparable(value);
        if (comparable != null) {
            NavigableMap<Comparable, Set<String>> map = indexes.computeIfAbsent(path, k -> new TreeMap<>());
            Set<String> ids = map.computeIfAbsent(comparable, v -> new HashSet<>());
            ids.add(id);
        }
        if (value instanceof String str) {
            textValues.computeIfAbsent(path, k -> new HashMap<>()).put(id, str.toLowerCase(Locale.ROOT));
        }
    }

    private void removeValue(String path, Object value, String id) {
        if (value == null || path == null) {
            return;
        }
        Comparable<?> comparable = normalizeComparable(value);
        if (comparable != null) {
            NavigableMap<Comparable, Set<String>> map = indexes.get(path);
            if (map != null) {
                Set<String> ids = map.get(comparable);
                if (ids != null) {
                    ids.remove(id);
                    if (ids.isEmpty()) {
                        map.remove(comparable);
                    }
                }
                if (map.isEmpty()) {
                    indexes.remove(path);
                }
            }
        }
        if (value instanceof String) {
            Map<String, String> values = textValues.get(path);
            if (values != null) {
                values.remove(id);
                if (values.isEmpty()) {
                    textValues.remove(path);
                }
            }
        }
    }

    private void traverse(String path, Object value, BiConsumer<String, Object> consumer) {
        if (value instanceof Map<?,?> map) {
            for (var entry : map.entrySet()) {
                Object key = entry.getKey();
                if (key == null) continue;
                String child = path == null ? key.toString() : path + "." + key;
                traverse(child, entry.getValue(), consumer);
            }
        } else if (value instanceof List<?> list) {
            for (int i = 0; i < list.size(); i++) {
                String child = path == null ? Integer.toString(i) : path + "." + i;
                traverse(child, list.get(i), consumer);
            }
        } else {
            consumer.accept(path, value);
        }
    }

    private String convertLikePattern(String pattern) {
        StringBuilder sb = new StringBuilder();
        sb.append('^');
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            switch (c) {
                case '%':
                    sb.append(".*");
                    break;
                case '_':
                    sb.append('.');
                    break;
                case '\\':
                    if (i + 1 < pattern.length()) {
                        sb.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
                    }
                    break;
                default:
                    if (".[]{}()*+-?^$|".indexOf(c) >= 0) {
                        sb.append('\\');
                    }
                    sb.append(c);
            }
        }
        sb.append('$');
        return sb.toString();
    }

    private Comparable<?> normalizeComparable(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (value instanceof Comparable<?> comparable) {
            return comparable;
        }
        return null;
    }
}
