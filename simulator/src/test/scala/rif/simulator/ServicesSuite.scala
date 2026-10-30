package rif.simulator

import rif.catalog.{CloudEvent, Pricing}

import java.time.{Duration, Instant}
import scala.util.Random

class ServicesSuite extends munit.FunSuite:

  val at   = Instant.parse("2026-10-30T12:00:00Z")
  val emit = Emitter("/test")

  def order(version: Int, seed: Long = 1): CloudEvent = Orders.placed(emit, Random(seed), version, at).event

  /** An order id that the issuer authorises, and one it declines. */
  val approved = Iterator.from(1).map(i => s"ord-$i").find(!Payments.declines(_)).get
  val declined = Iterator.from(1).map(i => s"ord-$i").find(Payments.declines).get

  def withOrderId(e: CloudEvent, id: String): CloudEvent =
    e.copy(data = ujson.copy(e.data).tap(_("orderId") = id))

  extension [A](a: A) def tap(f: A => Unit): A = { f(a); a }

  test("orders: v1 has a decimal total, v2 an amount object, and both agree with the items") {
    for seed <- 1L to 50L do
      val v1 = order(1, seed)
      val v2 = order(2, seed)
      assert(v1.data("total").strOpt.isDefined)
      assertEquals(v2.data("total")("currency").str, "USD")
      val items = v1.data("items").arr.map(i => BigDecimal(i("price").str) * i("qty").num.toInt).sum
      assertEquals(BigDecimal(v1.data("total").str), items)
      assertEquals(Pricing.default.price(v2).map(_.value), Pricing.default.price(v1).map(_.value))
  }

  test("orders: about 2,000 customers and believable totals") {
    val rng    = Random(7)
    val orders = List.fill(5000)(Orders.placed(emit, rng, 1, at).event)
    val totals = orders.map(e => BigDecimal(e.data("total").str))
    val mean   = totals.sum / totals.size
    assert(orders.map(_.data("customerId").str).distinct.size > 1800)
    assert(mean > 40 && mean < 300, s"mean order $mean")
    assert(totals.max > 1000, "some orders are big ones")
  }

  test("payments authorises a v1 order for its total") {
    val e   = withOrderId(order(1), approved)
    val out = Payments.handle(emit, e, patched = false, at).toOption.get
    assertEquals(out.map(o => (o.topic, o.key, o.event.eventType)), List((Payments.Authorized, approved, "PaymentAuthorized")))
    assertEquals(out.head.event.data("amount").str, e.data("total").str)
    assertEquals(out.head.event.redriveOf, None)
  }

  test("payments declines about one order in twenty, the same way every time") {
    val ids  = (1 to 10000).map(i => s"ord-$i")
    val rate = ids.count(Payments.declines).toDouble / ids.size
    assert(rate > 0.04 && rate < 0.06, rate)
    val out = Payments.handle(emit, withOrderId(order(1), declined), patched = false, at).toOption.get
    assertEquals(out.map(_.topic), List(Payments.Declined))
    assertEquals(out.head.event.data("reason").str, "card declined")
  }

  test("unpatched payments cannot read v2, and says why") {
    assertEquals(Payments.handle(emit, order(2), patched = false, at), Left("'total' is an object, expected a decimal string"))
  }

  test("patched payments reads both shapes") {
    for v <- List(1, 2) do
      val e = withOrderId(order(v), approved)
      val amount = Payments.handle(emit, e, patched = true, at).map(_.head.event.data("amount").str)
      assertEquals(amount, Pricing.default.price(e).map(p => money(p.value)), s"v$v")
  }

  test("a redriven payment carries the id of the order event it recovers") {
    val e   = withOrderId(order(2), approved)
    val out = Payments.handle(emit, e, patched = true, at, redrive = true).toOption.get
    assertEquals(out.head.event.redriveOf, Some(e.id))
  }

  test("payments refuses events that are not orders") {
    val stock = Inventory.handle(emit, order(1), at).toOption.get.head.event
    assertEquals(Payments.handle(emit, stock, patched = true, at), Left("payments cannot read StockReserved@v1"))
  }

  test("inventory reserves stock for both versions, valued from the items") {
    for v <- List(1, 2) do
      val e   = order(v, 3)
      val out = Inventory.handle(emit, e, at).toOption.get
      assertEquals(out.map(_.topic), List(Inventory.Reserved))
      assertEquals(Pricing.default.price(out.head.event).map(_.value), Pricing.default.price(e).map(_.value), s"v$v")
  }

  test("inventory dead-letters an order without items") {
    val e = order(1).copy(data = ujson.Obj("orderId" -> "o", "customerId" -> "c", "items" -> ujson.Arr()))
    assertEquals(Inventory.handle(emit, e, at), Left("no items"))
    val bad = e.copy(data = ujson.Obj("orderId" -> "o", "customerId" -> "c", "items" -> ujson.Arr(ujson.Obj("sku" -> "x", "qty" -> 0, "price" -> "1.00"))))
    assertEquals(Inventory.handle(emit, bad, at), Left("'qty' is not a positive whole number"))
  }

  def paymentAndStock(id: String): (CloudEvent, CloudEvent) =
    val e = withOrderId(order(1), id)
    (Payments.handle(emit, e, patched = false, at).toOption.get.head.event, Inventory.handle(emit, e, at).toOption.get.head.event)

  test("shipping dispatches once it has payment and stock, in either order") {
    val (pay, stock) = paymentAndStock(approved)
    for (first, second) <- List((pay, stock), (stock, pay)) do
      val s = Shipping(emit)
      assertEquals(s.handle(first, at), Right(Nil))
      assertEquals(s.pending, 1)
      val out = s.handle(second, at).toOption.get
      assertEquals(out.map(o => (o.topic, o.key)), List((Shipping.Dispatched, approved)))
      assertEquals(out.head.event.data("orderTotal").str, pay.data("amount").str)
      assertEquals(s.pending, 0)
  }

  test("shipping waits for a payment held up in a dead-letter queue, then dispatches the redriven one") {
    val e     = withOrderId(order(2), approved)
    val stock = Inventory.handle(emit, e, at).toOption.get.head.event
    val s     = Shipping(emit)
    assertEquals(s.handle(stock, at), Right(Nil))
    val redriven = Payments.handle(emit, e, patched = true, at.plusSeconds(120), redrive = true).toOption.get.head.event
    assertEquals(s.handle(redriven, at.plusSeconds(120)).map(_.map(_.topic)), Right(List(Shipping.Dispatched)))
  }

  test("shipping forgets orders that never complete") {
    val (_, stock) = paymentAndStock(declined)
    val s = Shipping(emit, ttl = Duration.ofMinutes(5))
    s.handle(stock, at)
    val (_, other) = paymentAndStock(approved)
    s.handle(other, at.plusSeconds(301))
    assertEquals(s.pending, 1)
  }

  test("shipping dead-letters what it does not read") {
    assertEquals(Shipping(emit).handle(order(1), at).left.map(identity), Left("shipping cannot read OrderPlaced@v1"))
  }
