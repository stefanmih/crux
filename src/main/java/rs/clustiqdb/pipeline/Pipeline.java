package rs.clustiqdb.pipeline;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Simple pipeline system that supports filter/map/group/reduce operations
 * on collections of data.
 */
public class Pipeline<T> {
    private final Stream<T> stream;

    public Pipeline(Collection<T> source) {
        this.stream = source.stream();
    }

    private Pipeline(Stream<T> stream) {
        this.stream = stream;
    }

    public Pipeline<T> filter(Predicate<T> predicate) {
        return new Pipeline<>(stream.filter(predicate));
    }

    public <R> Pipeline<R> map(Function<T, R> mapper) {
        return new Pipeline<>(stream.map(mapper));
    }

    public <K> Map<K, List<T>> groupBy(Function<T, K> classifier) {
        return stream.collect(Collectors.groupingBy(classifier));
    }

    public <R> R reduce(R identity, BiFunction<R, T, R> accumulator, BinaryOperator<R> combiner) {
        return stream.reduce(identity, accumulator, combiner);
    }

    public List<T> toList() {
        return stream.collect(Collectors.toList());
    }
}
