package inflight.catalog;

import java.util.List;
import java.util.function.Function;

/** A value, or every reason there is none. Nothing in the catalog is guessed, so a failure always says why. */
public sealed interface Result<T> {

    record Ok<T>(T value) implements Result<T> {}

    record Err<T>(List<String> errors) implements Result<T> {
        public Err {
            errors = List.copyOf(errors);
        }
    }

    static <T> Result<T> ok(T value) {
        return new Ok<>(value);
    }

    static <T> Result<T> err(String error) {
        return new Err<>(List.of(error));
    }

    static <T> Result<T> err(List<String> errors) {
        return new Err<>(errors);
    }

    default boolean isOk() {
        return this instanceof Ok;
    }

    /** The value; throws with the reasons when there is none. */
    default T value() {
        return switch (this) {
            case Ok<T> ok -> ok.value();
            case Err<T> err -> throw new IllegalStateException(String.join("; ", err.errors()));
        };
    }

    default List<String> errors() {
        return this instanceof Err<T> err ? err.errors() : List.of();
    }

    /** The reasons in one line, as the TypeScript build's single-error results read. */
    default String error() {
        return String.join("; ", errors());
    }

    default <U> Result<U> map(Function<? super T, ? extends U> f) {
        return switch (this) {
            case Ok<T> ok -> new Ok<>(f.apply(ok.value()));
            case Err<T> err -> new Err<>(err.errors());
        };
    }

    default <U> Result<U> flatMap(Function<? super T, Result<U>> f) {
        return switch (this) {
            case Ok<T> ok -> f.apply(ok.value());
            case Err<T> err -> new Err<>(err.errors());
        };
    }

    default T orElse(T other) {
        return this instanceof Ok<T> ok ? ok.value() : other;
    }
}
