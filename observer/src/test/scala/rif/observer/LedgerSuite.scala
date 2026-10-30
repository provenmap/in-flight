package rif.observer

import rif.catalog.{Catalog, EventRef, Priced}

import java.time.{Duration, Instant}

class LedgerSuite extends munit.FunSuite:

  val catalog = Catalog.default
  val t0      = Instant.parse("2026-10-30T12:00:00Z")

  def placed(id: String, order: String, customer: String, value: String, version: Int = 2): Priced =
    Priced(id, EventRef("OrderPlaced", version), "orders.placed", "orders", BigDecimal(value), customer, order, None)

  def authorized(id: String, order: String, customer: String, value: String, redriveOf: Option[String] = None): Priced =
    Priced(id, EventRef("PaymentAuthorized", 1), "payments.authorized", "payments", BigDecimal(value), customer, order, redriveOf)

  def dead(l: Ledger, offset: Long, p: Priced, at: Instant = t0): Msg =
    l.onDead("payments.dlq", Pos("payments.dlq", 0, offset), p, "'total' is an object", at)

  test("a dead letter is stuck money at payments") {
    val l = Ledger(catalog)
    val m = dead(l, 0, placed("e1", "o1", "c1", "100.00"))
    assert(m.isInstanceOf[Msg.Dead])
    assertEquals(m.json("district").str, "payments")
    dead(l, 1, placed("e2", "o2", "c1", "50.50"), t0.plusSeconds(1))
    val s = l.stats
    assertEquals((s.stuckValue, s.stuckEvents, s.stuckCustomers), (BigDecimal("150.50"), 2, 1))
    assertEquals(s.stuckSince, Some(t0))
    assertEquals(s.stuckByDistrict, Map("payments" -> BigDecimal("150.50")))
    assertEquals((s.atRiskValue, s.atRiskOrders, s.atRiskCustomers), (BigDecimal("150.50"), 2, 1))
  }

  test("a redriven success recovers its dead letter, with the time it was stuck") {
    val l = Ledger(catalog)
    dead(l, 0, placed("e1", "o1", "c1", "100.00"))
    dead(l, 1, placed("e2", "o2", "c2", "20.00"), t0.plusSeconds(5))
    val msgs = l.onFlow(Pos("payments.authorized", 0, 7), authorized("p1", "o1", "c1", "100.00", Some("e1")), t0.plusSeconds(30))
    assertEquals(msgs.map(_.json("type").str), List("event", "recovered"))
    assertEquals(msgs(1).json("afterMs").num, 30000.0)
    assertEquals(msgs(0).json("to").arr.map(_.str).toList, List("shipping"))
    assertEquals(msgs(0).json("redrive").bool, true)
    val s = l.stats
    assertEquals((s.stuckValue, s.recoveredValue, s.recoveredEvents), (BigDecimal("20.00"), BigDecimal("100.00"), 1))
    assertEquals(s.lastEpisode, None)

    l.onFlow(Pos("payments.authorized", 0, 8), authorized("p2", "o2", "c2", "20.00", Some("e2")), t0.plusSeconds(45))
    assertEquals(l.stats.lastEpisode, Some(Episode(BigDecimal("120.00"), 2, Duration.ofSeconds(45))))
    assertEquals(l.stats.atRiskValue, BigDecimal(0))
  }

  test("after a restart, dead letters the redrive already passed count as recovered, not stuck") {
    val l = Ledger(catalog)
    l.redriven("payments.dlq", 0, 2)
    val msgs = (0L to 2L).map(o => dead(l, o, placed(s"e$o", s"o$o", "c", "10.00")))
    assertEquals(msgs.map(_.json("type").str).toList, List("recovered", "recovered", "dead"))
    assert(msgs(0).json("afterMs").isNull)
    assertEquals((l.stats.stuckEvents, l.stats.recoveredEvents), (1, 2))
  }

  test("a redrive offset on one partition says nothing about another") {
    val l = Ledger(catalog)
    l.redriven("payments.dlq", 1, 100)
    assertEquals(dead(l, 0, placed("e", "o", "c", "1.00")).json("type").str, "dead")
  }

  test("a redriven event that overtakes its dead letter still recovers it") {
    val l = Ledger(catalog)
    assertEquals(l.onFlow(Pos("payments.authorized", 0, 0), authorized("p1", "o1", "c1", "9.00", Some("e1")), t0).size, 1)
    assertEquals(dead(l, 0, placed("e1", "o1", "c1", "9.00")).json("type").str, "recovered")
    assertEquals(l.stats.stuckEvents, 0)
  }

  test("lag: what a group has not committed, valued from the events the observer saw") {
    val l = Ledger(catalog)
    for o <- 0L to 4L do l.onFlow(Pos("orders.placed", 0, o), placed(s"e$o", s"o$o", s"c${o % 2}", "10.00", 1), t0)
    l.onOffsets("payments-service", Map(("orders.placed", 0) -> 2L), Map(("orders.placed", 0) -> 5L))
    l.onOffsets("inventory-service", Map(("orders.placed", 0) -> 5L), Map.empty)
    val s = l.stats
    assertEquals(s.lag("payments-service"), GroupLag(3, BigDecimal("30.00"), 2))
    assertEquals(s.lag("inventory-service"), GroupLag(0, BigDecimal(0), 0))
    assertEquals(s.lag("shipping-service").events, 0L)
    assertEquals((s.atRiskValue, s.atRiskOrders, s.atRiskCustomers), (BigDecimal("30.00"), 3, 2))
  }

  test("events past the polled end are in flight, not lag") {
    val l = Ledger(catalog)
    for o <- 0L to 4L do l.onFlow(Pos("orders.placed", 0, o), placed(s"e$o", s"o$o", "c", "10.00", 1), t0)
    l.onOffsets("payments-service", Map(("orders.placed", 0) -> 2L), Map(("orders.placed", 0) -> 3L))
    assertEquals(l.stats.lag("payments-service"), GroupLag(1, BigDecimal("10.00"), 1))
  }

  test("lag counts events the observer did not see, but cannot value them") {
    val l = Ledger(catalog)
    l.onOffsets("payments-service", Map(("orders.placed", 1) -> 10L), Map(("orders.placed", 1) -> 25L))
    assertEquals(l.stats.lag("payments-service"), GroupLag(15, BigDecimal(0), 0))
  }

  test("an order held up in two places is at risk once, at its largest value") {
    val l = Ledger(catalog)
    dead(l, 0, placed("e1", "o1", "c1", "100.00"))
    l.onFlow(Pos("orders.placed", 0, 0), placed("e1", "o1", "c1", "100.00"), t0)
    l.onFlow(Pos("inventory.reserved", 0, 0),
      Priced("s1", EventRef("StockReserved", 1), "inventory.reserved", "inventory", BigDecimal("95.00"), "c1", "o1", None), t0)
    l.onOffsets("inventory-service", Map(("orders.placed", 0) -> 0L), Map(("orders.placed", 0) -> 1L))
    l.onOffsets("shipping-service", Map(("inventory.reserved", 0) -> 0L), Map(("inventory.reserved", 0) -> 1L))
    val s = l.stats
    assertEquals(s.stuckValue + s.lagValue, BigDecimal("295.00"))
    assertEquals((s.atRiskValue, s.atRiskOrders, s.atRiskCustomers), (BigDecimal("100.00"), 1, 1))
  }

  test("events every reader has committed are forgotten") {
    val l = Ledger(catalog)
    for o <- 0L to 9L do l.onFlow(Pos("orders.placed", 0, o), placed(s"e$o", s"o$o", "c", "1.00", 1), t0)
    l.onOffsets("payments-service", Map(("orders.placed", 0) -> 10L), Map(("orders.placed", 0) -> 10L))
    l.onOffsets("inventory-service", Map(("orders.placed", 0) -> 6L), Map.empty)
    assertEquals(l.stats.lag("inventory-service"), GroupLag(4, BigDecimal("4.00"), 1))
    // Payments rewinds (a reset, say): only what inventory still holds can be valued.
    l.onOffsets("payments-service", Map(("orders.placed", 0) -> 0L), Map.empty)
    assertEquals(l.stats.lag("payments-service"), GroupLag(10, BigDecimal("4.00"), 1))
  }

  test("flow topics nobody reads are not remembered, and go nowhere") {
    val l = Ledger(catalog)
    val declined = Priced("d1", EventRef("PaymentDeclined", 1), "payments.declined", "payments", BigDecimal("5.00"), "c", "o", None)
    assertEquals(l.onFlow(Pos("payments.declined", 0, 0), declined, t0).head.json("to").arr.toList, Nil)
  }
