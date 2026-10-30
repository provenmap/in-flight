package rif.catalog

import java.nio.file.Paths

class CatalogSuite extends munit.FunSuite:

  val file = Catalog.load(Paths.get("catalog/order-to-cash.json"))
  val c    = file.toOption.get

  def edit(f: ujson.Value => Unit): Either[List[String], Catalog] =
    val json = ujson.read(java.nio.file.Files.readString(Paths.get("catalog/order-to-cash.json")))
    f(json)
    Catalog.fromJson(json)

  test("the shipped catalog loads, and the classpath copy matches the file") {
    assert(file.isRight, file)
    assertEquals(Catalog.default, c)
  }

  test("districts, topics and dead-letter queues") {
    assertEquals(c.districts.map(_.id), List("orders", "payments", "inventory", "shipping", "customers"))
    assertEquals(c.districts.filter(_.sink).map(_.id), List("customers"))
    assertEquals(c.topics, List("orders.placed", "payments.authorized", "payments.declined", "inventory.reserved", "shipping.dispatched"))
    assertEquals(c.dlqs, List("payments.dlq", "inventory.dlq", "shipping.dlq"))
    assertEquals(c.services.flatMap(_.group), List("payments-service", "inventory-service", "shipping-service"))
  }

  test("who reads what") {
    assertEquals(c.readersOf("orders.placed").map(_.district), List("payments", "inventory"))
    assertEquals(c.readersOf("payments.authorized").map(_.district), List("shipping"))
    assertEquals(c.readersOf("payments.declined"), Nil)
    assertEquals(c.readersOf("shipping.dispatched").map(_.name), List("customers"))
    assertEquals(c.ownerOfDlq("payments.dlq").flatMap(_.group), Some("payments-service"))
    assertEquals(c.districtOf("inventory.reserved"), Some("inventory"))
    assertEquals(c.redriveGroupOf("payments.dlq"), Some("payments-service-redrive"))
    assertEquals(c.redriveGroupOf("inventory.dlq"), None)
  }

  test("a redrive group needs a dead-letter queue, and cannot reuse a service's group") {
    assertEquals(edit(_("consumers")(3)("redrive") = "customers-redrive"),
      Left(List("consumer customers: a redrive group needs a dead-letter queue")))
    assertEquals(edit(_("consumers")(0)("redrive") = "inventory-service"),
      Left(List("consumer group 'inventory-service' is declared twice")))
  }

  test("both OrderPlaced versions share a topic but declare their value in different places") {
    val v1 = c.find(EventRef("OrderPlaced", 1)).get
    val v2 = c.find(EventRef("OrderPlaced", 2)).get
    assertEquals(v1.topic, v2.topic)
    assertEquals(v1.value.show, "total")
    assertEquals(v2.value.show, "total.amount")
  }

  test("an unknown district is rejected") {
    val res = edit(_("events")(2)("district") = "billing")
    assertEquals(res, Left(List("PaymentAuthorized@v1: unknown district 'billing'")))
  }

  test("a consumer of an unknown topic is rejected") {
    val res = edit(_("consumers")(2)("reads").arr += "billing.invoices")
    assertEquals(res, Left(List("consumer shipping-service: reads unknown topic 'billing.invoices'")))
  }

  test("a version declared twice is rejected") {
    val res = edit(j => j("events").arr += j("events")(0))
    assertEquals(res, Left(List("OrderPlaced@v1 is declared twice")))
  }

  test("a dead-letter queue without a consumer group is rejected") {
    val res = edit(_("consumers")(3)("dlq") = "customers.dlq")
    assertEquals(res, Left(List("consumer customers: a dead-letter queue needs a consumer group")))
  }

  test("a sink cannot produce") {
    val res = edit(_("events")(5)("district") = "customers")
    assertEquals(res, Left(List("OrderDispatched@v1: a sink cannot produce events")))
  }

  test("a bad value path and a missing version are both reported") {
    val res = edit { j =>
      j("events")(0)("value") = "total..amount"
      j("events")(1).obj.remove("version")
    }
    assertEquals(res.left.map(_.toSet), Left(Set(
      "events[0]: value: 'total..amount' is not a dotted field path",
      "events[1]: 'version' must be a whole number"
    )))
  }
