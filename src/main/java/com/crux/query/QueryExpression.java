package com.crux.query;

import com.crux.index.IndexManager;
import com.crux.store.DocumentStore;
import com.crux.store.Entity;

import java.util.*;
import java.util.function.Predicate;

/**
 * Functional interface representing a query expression that can
 * evaluate itself using indexes and the document store.
 */
public interface QueryExpression {
    Set<String> evaluate(IndexManager indexes, DocumentStore store);

    enum Operator {
        EQ("=="),
        NE("!="),
        GT(">"),
        GTE(">="),
        LT("<"),
        LTE("<=");

        private final String symbol;

        Operator(String symbol) {
            this.symbol = symbol;
        }

        public String symbol() {
            return symbol;
        }

        public static Optional<Operator> fromSymbol(String symbol) {
            for (Operator op : values()) {
                if (op.symbol.equals(symbol)) {
                    return Optional.of(op);
                }
            }
            return Optional.empty();
        }
    }

    static QueryExpression field(String field, Operator op, Comparable value) {
        return (indexes, store) -> {
            return switch (op) {
                case EQ -> indexes.searchEquals(field, value);
                case GT -> indexes.searchGreaterThan(field, value);
                case GTE -> indexes.searchGreaterOrEquals(field, value);
                case LT -> indexes.searchLessThan(field, value);
                case LTE -> indexes.searchLessOrEquals(field, value);
                case NE -> {
                    Set<String> all = new HashSet<>(store.getAllIds());
                    all.removeAll(indexes.searchEquals(field, value));
                    yield all;
                }
            };
        };
    }

    static QueryExpression contains(String field, String substring) {
        return (indexes, store) -> {
            return indexes.searchContains(field, substring);
        };
    }

    static QueryExpression like(String field, String pattern) {
        return (indexes, store) -> indexes.searchLike(field, pattern);
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

    static QueryExpression not(QueryExpression expr) {
        return (indexes, store) -> {
            Set<String> all = new HashSet<>(store.getAllIds());
            all.removeAll(expr.evaluate(indexes, store));
            return all;
        };
    }

    static QueryExpression fromPredicate(Predicate<Entity> predicate) {
        return (indexes, store) -> {
            Set<String> result = new HashSet<>();
            for (Entity entity : store.findAll()) {
                if (predicate.test(entity)) {
                    result.add(entity.getId());
                }
            }
            return result;
        };
    }
}
