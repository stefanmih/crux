package com.crux.store;

import com.crux.index.IndexManager;
import com.crux.query.QueryExpression;
import com.crux.version.VersioningManager;

import java.util.*;
import java.util.stream.Collectors;

/**
 * In-memory store for schemaless entities with automatic indexing
 * and simple versioning support.
 */
public class DocumentStore {
    private final Map<String, Entity> data = new HashMap<>();
    private final IndexManager indexManager = new IndexManager();
    private final VersioningManager versioningManager = new VersioningManager();

    public void insert(Entity entity) {
        data.put(entity.getId(), entity);
        indexManager.index(entity);
        versioningManager.recordInsert(entity);
    }

    public void update(String id, Map<String, Object> newFields) {
        Entity old = data.get(id);
        if (old != null) {
            indexManager.remove(old);
        }
        Entity entity = new Entity(id, newFields);
        data.put(id, entity);
        indexManager.index(entity);
        versioningManager.recordUpdate(id, newFields);
    }

    /**
     * Performs a partial update merging the provided fields with
     * the existing entity state.
     */
    public void updatePartial(String id, Map<String, Object> fields) {
        Entity current = data.get(id);
        Map<String, Object> merged = current == null
                ? new HashMap<>()
                : new HashMap<>(current.getFields());
        merged.putAll(fields);
        update(id, merged);
    }

    public void delete(String id) {
        Entity entity = data.remove(id);
        if (entity != null) {
            indexManager.remove(entity);
            versioningManager.recordDelete(id);
        }
    }

    public List<Entity> query(QueryExpression expr) {
        Set<String> ids = expr.evaluate(indexManager, this);
        return ids.stream().map(data::get).collect(Collectors.toList());
    }

    public Collection<Entity> findAll() {
        return data.values();
    }

    public Entity get(String id) {
        return data.get(id);
    }

    public Entity getAt(String id, long timestamp) {
        Map<String, Object> fields = versioningManager.getAt(id, timestamp);
        return fields == null ? null : new Entity(id, fields);
    }

    public List<Map<String, Object>> getHistory(String id) {
        return versioningManager.getHistory(id);
    }

    /**
     * Finds entities with vectors most similar to the entity with the given id.
     * Similarity is measured using cosine similarity of the "vector" field.
     */
    public List<Entity> findSimilar(String id, int topN) {
        Entity base = data.get(id);
        if (base == null) return Collections.emptyList();
        List<Double> baseVec = getVector(base);
        if (baseVec == null) return Collections.emptyList();
        List<Entity> others = new ArrayList<>();
        for (Entity e : data.values()) {
            if (!e.getId().equals(id) && getVector(e) != null) {
                others.add(e);
            }
        }
        others.sort((a, b) -> Double.compare(
                similarity(baseVec, getVector(b)),
                similarity(baseVec, getVector(a))));
        if (topN < others.size()) {
            return new ArrayList<>(others.subList(0, topN));
        }
        return others;
    }

    @SuppressWarnings("unchecked")
    private List<Double> getVector(Entity e) {
        Object v = e.get("vector");
        if (v instanceof List<?> l) {
            List<Double> out = new ArrayList<>();
            for (Object o : l) out.add(((Number) o).doubleValue());
            return out;
        }
        return null;
    }

    private double similarity(List<Double> a, List<Double> b) {
        if (b == null || a.size() != b.size()) return -1;
        double dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.size(); i++) {
            double x = a.get(i);
            double y = b.get(i);
            dot += x * y;
            normA += x * x;
            normB += y * y;
        }
        if (normA == 0 || normB == 0) return -1;
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
