package rif.catalog

import java.time.Instant

class PricingSuite extends munit.FunSuite:

  val pricing = Pricing.default
  val at      = Instant.parse("2026-10-30T12:00:00Z")

  def event(tpe: String, version: Int, data: ujson.Obj): CloudEvent =
    CloudEvent("e-1", "/test", tpe, version, at, 1, data)

  def placedV1 = event("OrderPlaced", 1, ujson.Obj("orderId" -> "o-1", "customerId" -> "c-42", "total" -> "129.90", "currency" -> "USD"))
  def placedV2 = event("OrderPlaced", 2, ujson.Obj("orderId" -> "o-2", "customerId" -> "c-7",
    "total" -> ujson.Obj("amount" -> "1049.00", "currency" -> "USD")))

  test("OrderPlaced v1 is worth its total") {
    assertEquals(pricing.price(placedV1),
      Right(Priced("e-1", EventRef("OrderPlaced", 1), "orders.placed", "orders", BigDecimal("129.90"), "c-42", "o-1", None)))
  }

  test("OrderPlaced v2 is worth total.amount") {
    val p = pricing.price(placedV2).toOption.get
    assertEquals((p.district, p.value, p.customer, p.orderId), ("orders", BigDecimal("1049.00"), "c-7", "o-2"))
  }

  test("every downstream event is priced in its own district") {
    val cases = List(
      ("PaymentAuthorized", "amount", "payments"),
      ("PaymentDeclined", "amount", "payments"),
      ("StockReserved", "value", "inventory"),
      ("OrderDispatched", "orderTotal", "shipping")
    )
    for (tpe, field, district) <- cases do
      val p = pricing.price(event(tpe, 1, ujson.Obj("orderId" -> "o-3", "customerId" -> "c-1", field -> "15.25")))
      assertEquals(p.map(x => (x.district, x.value)), Right((district, BigDecimal("15.25"))), tpe)
  }

  test("a JSON number is an amount too") {
    val p = pricing.price(event("PaymentAuthorized", 1, ujson.Obj("orderId" -> "o", "customerId" -> "c", "amount" -> 12.5)))
    assertEquals(p.map(_.value), Right(BigDecimal("12.5")))
  }

  test("v2 data read with the v1 path is not guessed at") {
    val mislabelled = placedV2.copy(version = 1)
    assertEquals(pricing.price(mislabelled).map(_.value), Left("OrderPlaced@v1 (e-1): no amount at 'total'"))
  }

  test("an undeclared type or version is an error") {
    assertEquals(pricing.price(placedV1.copy(version = 3)), Left("OrderPlaced@v3 (e-1) is not in the catalog"))
    assertEquals(pricing.price(placedV1.copy(eventType = "OrderAmended")), Left("OrderAmended@v1 (e-1) is not in the catalog"))
  }

  test("a missing customer or order is an error") {
    val noCustomer = event("StockReserved", 1, ujson.Obj("orderId" -> "o", "value" -> "1.00"))
    assertEquals(pricing.price(noCustomer), Left("StockReserved@v1 (e-1): no customer at 'customerId'"))
  }

  test("a redriven event keeps the id of the one it recovers, through the wire format") {
    val redriven = event("PaymentAuthorized", 1, ujson.Obj("orderId" -> "o", "customerId" -> "c", "amount" -> "9.99"))
      .copy(id = "e-2", redriveOf = Some("e-1"))
    val back = CloudEvents.parse(redriven.json)
    assertEquals(back, Right(redriven))
    assertEquals(pricing.price(redriven.json.getBytes("UTF-8")).map(_.redriveOf), Right(Some("e-1")))
  }

  test("a fresh event round-trips and has no redrive marker") {
    val e = CloudEvents("/orders", "OrderPlaced", 1, at, 7, ujson.Obj("orderId" -> "o", "customerId" -> "c", "total" -> "1.00"))
    assertEquals(CloudEvents.parse(e.json), Right(e))
    assert(!e.json.contains("redriveof"))
  }

  test("an envelope without a schemaversion or data is rejected") {
    assertEquals(CloudEvents.parse("""{"id":"x","source":"s","type":"T","data":{}}"""), Left("T (x) has no whole schemaversion"))
    assertEquals(CloudEvents.parse("""{"id":"x","source":"s","type":"T","schemaversion":1}"""), Left("T (x) has no data object"))
    assert(CloudEvents.parse("not json").isLeft)
  }
