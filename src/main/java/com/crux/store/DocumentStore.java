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
}
