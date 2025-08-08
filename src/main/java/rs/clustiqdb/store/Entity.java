package rs.clustiqdb.store;

import java.util.Map;

/**
 * Represents a single schemaless entity stored in the database.
 */
public class Entity {
    private final String id;
    private final Map<String, Object> fields;

    public Entity(String id, Map<String, Object> fields) {
        this.id = id;
        this.fields = fields;
    }

    public String getId() {
        return id;
    }

    public Object get(String field) {
        return fields.get(field);
    }

    public Map<String, Object> getFields() {
        return fields;
    }
}
