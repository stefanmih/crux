package com.crux.store;

import com.crux.index.IndexManager;
import com.crux.query.QueryExpression;
import com.crux.version.VersioningManager;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * In-memory store for schemaless entities with automatic indexing
 * and simple versioning support.
 */
public class DocumentStore {
    private static final Logger LOGGER = Logger.getLogger(DocumentStore.class.getName());

    private final Map<String, Entity> data = new HashMap<>();
    private final IndexManager indexManager = new IndexManager();
    private final VersioningManager versioningManager = new VersioningManager();

    public void insert(Entity entity) {
        if (entity == null) {
            LOGGER.severe("Attempted to insert null entity");
            throw new IllegalArgumentException("entity cannot be null");
        }
        try {
            data.put(entity.getId(), entity);
            indexManager.index(entity);
            versioningManager.recordInsert(entity);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to insert entity " + entity.getId(), e);
            throw new RuntimeException(e);
        }
    }

    public void update(String id, Map<String, Object> newFields) {
        if (id == null || newFields == null) {
            LOGGER.severe("Update called with null id or fields");
            throw new IllegalArgumentException("id and fields must be non-null");
        }
        try {
            Entity old = data.get(id);
            if (old != null) {
                indexManager.remove(old);
            }
            Entity entity = new Entity(id, newFields);
            data.put(id, entity);
            indexManager.index(entity);
            versioningManager.recordUpdate(id, newFields);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update entity " + id, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Performs a partial update merging the provided fields with
     * the existing entity state.
     */
    public void updatePartial(String id, Map<String, Object> fields) {
        if (id == null || fields == null) {
            LOGGER.severe("updatePartial called with null id or fields");
            throw new IllegalArgumentException("id and fields must be non-null");
        }
        try {
            Entity current = data.get(id);
            Map<String, Object> merged = current == null
                    ? new HashMap<>()
                    : new HashMap<>(current.getFields());
            merged.putAll(fields);
            update(id, merged);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to partially update entity " + id, e);
            throw new RuntimeException(e);
        }
    }

    public void delete(String id) {
        if (id == null) {
            LOGGER.severe("delete called with null id");
            throw new IllegalArgumentException("id must be non-null");
        }
        try {
            Entity entity = data.remove(id);
            if (entity != null) {
                indexManager.remove(entity);
                versioningManager.recordDelete(id);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to delete entity " + id, e);
            throw new RuntimeException(e);
        }
    }

    public List<Entity> query(QueryExpression expr) {
        if (expr == null) {
            LOGGER.severe("query called with null expression");
            throw new IllegalArgumentException("expression must be non-null");
        }
        try {
            Set<String> ids = expr.evaluate(indexManager, this);
            return ids.stream().map(data::get).collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Query failed", e);
            throw new RuntimeException(e);
        }
    }

    public Collection<Entity> findAll() {
        return data.values();
    }

    public Entity get(String id) {
        if (id == null) {
            LOGGER.warning("get called with null id");
            return null;
        }
        return data.get(id);
    }

    public Entity getAt(String id, long timestamp) {
        if (id == null) {
            LOGGER.warning("getAt called with null id");
            return null;
        }
        try {
            Map<String, Object> fields = versioningManager.getAt(id, timestamp);
            return fields == null ? null : new Entity(id, fields);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get entity at time for id " + id, e);
            throw new RuntimeException(e);
        }
    }

    public List<Map<String, Object>> getHistory(String id) {
        if (id == null) {
            LOGGER.warning("getHistory called with null id");
            return Collections.emptyList();
        }
        try {
            return versioningManager.getHistory(id);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get history for id " + id, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Finds entities with vectors most similar to the entity with the given id.
     * Similarity is measured using cosine similarity of the "vector" field.
     */
    public List<Entity> findSimilar(String id, int topN) {
        if (id == null || topN <= 0) {
            LOGGER.warning("findSimilar called with invalid arguments");
            return Collections.emptyList();
        }
        try {
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
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to find similar entities for id " + id, e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Double> getVector(Entity e) {
        try {
            Object v = e.get("vector");
            if (v instanceof List<?> l) {
                List<Double> out = new ArrayList<>();
                for (Object o : l) out.add(((Number) o).doubleValue());
                return out;
            }
            return null;
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "Failed to extract vector from entity " + e.getId(), ex);
            return null;
        }
    }

    private double similarity(List<Double> a, List<Double> b) {
        if (b == null || a == null || a.size() != b.size()) {
            LOGGER.warning("Cannot compute similarity for mismatched vectors");
            return -1;
        }
        try {
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
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to compute similarity", e);
            return -1;
        }
    }
}
