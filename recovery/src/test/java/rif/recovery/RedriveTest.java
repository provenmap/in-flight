package rif.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import rif.catalog.Result;

class RedriveTest {

    @Test
    void theSameFailureOnManyRecordsIsOneCause() {
        String a = Redrive.cause("cannot read 'total' of order ord-1 at offset 17: expected number");
        String b = Redrive.cause("cannot read 'total' of order ord-99 at offset 4: expected number");
        assertEquals(a, b);
        assertEquals("no error given", Redrive.cause(null));
        assertTrue(Redrive.cause("x".repeat(400)).length() <= 161);
    }

    @Test
    void redrivenOffsetsBeyondTheCommitAreKeptAsRanges() {
        TreeSet<Long> s = new TreeSet<>(List.of(17L, 18L, 19L, 20L, 25L, 30L, 31L));
        String meta = Redrive.format(s);
        assertEquals("rif-redriven:17-20,25,30-31", meta);
        assertEquals(s, Redrive.parse(meta));
        assertTrue(Redrive.parse("someone else's").isEmpty());
        assertEquals("", Redrive.format(new TreeSet<>()));
    }

    @Test
    void filtersByErrorAndTime() {
        Instant at = Instant.parse("2026-11-13T10:00:00Z");
        Result<rif.catalog.Records.Decoded> none = Result.err("x");
        assertTrue(Redrive.matches(Redrive.Filter.ALL, null, at, none, null, null));
        assertTrue(Redrive.matches(new Redrive.Filter("TOTAL", null, null, null), "cannot read total", at, none, null, null));
        assertFalse(Redrive.matches(new Redrive.Filter("timeout", null, null, null), "cannot read total", at, none, null, null));
        assertFalse(Redrive.matches(new Redrive.Filter(null, null, at.plusSeconds(1), null), "e", at, none, null, null));
        assertTrue(Redrive.matches(new Redrive.Filter(null, null, at, at), "e", at, none, null, null));
        assertFalse(Redrive.matches(new Redrive.Filter(null, "o1", null, null), "e", at, none, null, null), "an order filter needs a priced record");
    }
}
