package rs.clustiqdb.query;

import rs.clustiqdb.index.IndexManager;
import rs.clustiqdb.store.DocumentStore;

import java.util.*;

/**
 * Functional interface representing a query expression that can
 * evaluate itself using indexes and the document store.
 */
public interface QueryExpression {
    Set<String> evaluate(IndexManager indexes, DocumentStore store);

    enum Operator { EQ, GT, LT }

    static QueryExpression field(String field, Operator op, Comparable value) {
        return (indexes, store) -> {
            switch (op) {
                case EQ:
                    return indexes.searchEquals(field, value);
                case GT:
                    return indexes.searchGreaterThan(field, value);
                case LT:
                    return indexes.searchLessThan(field, value);
                default:
                    return Collections.emptySet();
            }
        };
    }

    static QueryExpression contains(String field, String substring) {
        return (indexes, store) -> {
            Set<String> result = new HashSet<>();
            for (var entity : store.findAll()) {
                Object v = entity.get(field);
                if (v instanceof String && ((String) v).contains(substring)) {
                    result.add(entity.getId());
                }
            }
            return result;
        };
    }

    static QueryExpression and(QueryExpression... exprs) {
        return (indexes, store) -> {
            if (exprs.length == 0) {
                return Collections.emptySet();
            }
            Set<String> result = null;
            for (QueryExpression e : exprs) {
                Set<String> set = e.evaluate(indexes, store);
                if (result == null) {
                    result = new HashSet<>(set);
                } else {
                    result.retainAll(set);
                }
            }
            return result == null ? Collections.emptySet() : result;
        };
    }

    static QueryExpression or(QueryExpression... exprs) {
        return (indexes, store) -> {
            Set<String> result = new HashSet<>();
            for (QueryExpression e : exprs) {
                result.addAll(e.evaluate(indexes, store));
            }
            return result;
        };
    }
}
