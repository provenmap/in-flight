package rif.catalog;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** A predicate over the event catalog. It denotes a set of event type versions. */
public sealed interface Predicate {

    record Changes(AttrRef attribute) implements Predicate {}

    record Emits(String eventType) implements Predicate {}

    record About(String entity) implements Predicate {}

    record And(Predicate left, Predicate right) implements Predicate {}

    record Or(Predicate left, Predicate right) implements Predicate {}

    record Not(Predicate inner) implements Predicate {}

    /** Written so it parses back to itself: {@code not} binds tightest, then {@code and}, then {@code or}. */
    default String show() {
        return switch (this) {
            case Changes c -> "changes(" + c.attribute().show() + ")";
            case Emits e -> "emits(" + e.eventType() + ")";
            case About a -> "about(" + a.entity() + ")";
            case And a -> operand(a.left(), this) + " and " + operand(a.right(), this);
            case Or o -> operand(o.left(), this) + " or " + operand(o.right(), this);
            case Not n -> "not " + operand(n.inner(), this);
        };
    }

    private static String operand(Predicate child, Predicate parent) {
        boolean wrap = (parent instanceof And && child instanceof Or)
                || (parent instanceof Not && (child instanceof And || child instanceof Or));
        return wrap ? "(" + child.show() + ")" : child.show();
    }

    /** Entities this predicate names through {@code changes} and {@code about}, outside any {@code not}. */
    default Set<String> namedEntities() {
        Set<String> out = new LinkedHashSet<>();
        switch (this) {
            case Changes c -> out.add(c.attribute().entity());
            case About a -> out.add(a.entity());
            case And a -> {
                out.addAll(a.left().namedEntities());
                out.addAll(a.right().namedEntities());
            }
            case Or o -> {
                out.addAll(o.left().namedEntities());
                out.addAll(o.right().namedEntities());
            }
            default -> {}
        }
        return out;
    }

    /** Every attribute the predicate asks about with {@code changes}, outside any {@code not}. */
    default List<AttrRef> watchedAttributes() {
        List<AttrRef> out = new ArrayList<>();
        switch (this) {
            case Changes c -> out.add(c.attribute());
            case And a -> {
                out.addAll(a.left().watchedAttributes());
                out.addAll(a.right().watchedAttributes());
            }
            case Or o -> {
                out.addAll(o.left().watchedAttributes());
                out.addAll(o.right().watchedAttributes());
            }
            default -> {}
        }
        return out;
    }

    /** Whether one event type version is in the set this predicate denotes. */
    default boolean matches(Model.Event e) {
        return switch (this) {
            case Changes c -> !e.affects(c.attribute()).isEmpty();
            case Emits em -> e.ref().name().equals(em.eventType());
            case About a -> e.keys().containsKey(a.entity());
            case And a -> a.left().matches(e) && a.right().matches(e);
            case Or o -> o.left().matches(e) || o.right().matches(e);
            case Not n -> !n.inner().matches(e);
        };
    }

    /** The intent language: {@code changes(Order.total) and not emits(OrderPlaced)}. */
    static Result<Predicate> parse(String text) {
        return IntentLanguage.parse(text);
    }
}
