package rif.simulator

import rif.catalog.CloudEvent

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/** What payments needs from an order to charge it. */
final case class Charge(orderId: String, customerId: String, amount: BigDecimal, currency: String)

/**
 * payments-service: charges every order placed. It reads `total` as a decimal string, so an `OrderPlaced` v2 fails
 * and is dead-lettered. Patched, it reads both shapes. About one charge in twenty is declined.
 */
object Payments:
  val Group      = "payments-service"
  val Dlq        = "payments.dlq"
  val Authorized = "payments.authorized"
  val Declined   = "payments.declined"

  def read(e: CloudEvent, patched: Boolean): Either[String, Charge] =
    val d = e.data
    for
      _        <- Either.cond(Orders.isPlaced(e), (), s"payments cannot read ${e.ref.show}")
      orderId  <- Read.str(d, "orderId")
      customer <- Read.str(d, "customerId")
      total    <- Read.field(d, "total")
      charge <- total match
        case _: ujson.Obj if patched =>
          for amount <- Read.decimal(total, "amount"); currency <- Read.str(total, "currency")
          yield Charge(orderId, customer, amount, currency)
        case _ =>
          for amount <- Read.decimal(d, "total"); currency <- Read.str(d, "currency")
          yield Charge(orderId, customer, amount, currency)
    yield charge

  /** The card issuer's answer, fixed per order so a redrive decides the same way. */
  def declines(orderId: String): Boolean = math.floorMod(MurmurHash3.stringHash(orderId), 100) < 5

  def handle(emit: Emitter, e: CloudEvent, patched: Boolean, now: Instant, redrive: Boolean = false): Either[String, List[Out]] =
    read(e, patched).map { c =>
      val base = ujson.Obj("orderId" -> c.orderId, "customerId" -> c.customerId, "amount" -> money(c.amount), "currency" -> c.currency)
      val redriveOf = Option.when(redrive)(e.id)
      if declines(c.orderId) then
        base("reason") = "card declined"
        base("declinedAt") = now.toString
        List(Out(Declined, c.orderId, emit("PaymentDeclined", 1, now, base, redriveOf)))
      else
        base("paymentId") = s"pay-${c.orderId.stripPrefix("ord-")}"
        base("authorizedAt") = now.toString
        List(Out(Authorized, c.orderId, emit("PaymentAuthorized", 1, now, base, redriveOf)))
    }

/** inventory-service: reserves stock for every order placed. It reads only the items, so v2 does not bother it. */
object Inventory:
  val Group    = "inventory-service"
  val Dlq      = "inventory.dlq"
  val Reserved = "inventory.reserved"

  def handle(emit: Emitter, e: CloudEvent, now: Instant): Either[String, List[Out]] =
    val d = e.data
    for
      _        <- Either.cond(Orders.isPlaced(e), (), s"inventory cannot read ${e.ref.show}")
      orderId  <- Read.str(d, "orderId")
      customer <- Read.str(d, "customerId")
      items    <- Read.field(d, "items").flatMap(_.arrOpt.toRight("'items' is not an array"))
      lines <- items.toList.foldLeft(Either.cond(items.nonEmpty, List.empty[(String, Int, BigDecimal)], "no items")) { (acc, it) =>
        for
          ls    <- acc
          sku   <- Read.str(it, "sku")
          qty   <- Read.field(it, "qty").flatMap(_.numOpt.filter(q => q.isWhole && q > 0).map(_.toInt).toRight("'qty' is not a positive whole number"))
          price <- Read.decimal(it, "price")
        yield ls :+ (sku, qty, price)
      }
    yield
      val value = lines.map((_, q, p) => p * q).sum
      val data = ujson.Obj(
        "reservationId" -> s"res-${orderId.stripPrefix("ord-")}", "orderId" -> orderId, "customerId" -> customer,
        "items" -> ujson.Arr.from(lines.map((s, q, _) => ujson.Obj("sku" -> s, "qty" -> q))),
        "value" -> money(value), "reservedAt" -> now.toString
      )
      List(Out(Reserved, orderId, emit("StockReserved", 1, now, data)))

/**
 * shipping-service: dispatches an order once it has both its payment and its stock, in either order. An order
 * that never completes, such as a declined one whose stock was reserved, is dropped after `ttl`.
 */
final class Shipping(emit: Emitter, ttl: Duration = Duration.ofMinutes(30)):
  import Shipping.*

  private final case class Waiting(customer: String, payment: Option[BigDecimal], stock: Boolean, since: Instant)
  private val waiting = mutable.LinkedHashMap.empty[String, Waiting]

  def pending: Int = waiting.size

  def handle(e: CloudEvent, now: Instant): Either[String, List[Out]] =
    expire(now)
    val d = e.data
    for
      orderId  <- Read.str(d, "orderId")
      customer <- Read.str(d, "customerId")
      got <- e.eventType match
        case "PaymentAuthorized" => Read.decimal(d, "amount").map(a => (w: Waiting) => w.copy(payment = Some(a)))
        case "StockReserved"     => Right((w: Waiting) => w.copy(stock = true))
        case _                   => Left(s"shipping cannot read ${e.ref.show}")
    yield
      val w = got(waiting.getOrElse(orderId, Waiting(customer, None, false, now)))
      w.payment match
        case Some(amount) if w.stock =>
          waiting.remove(orderId)
          val data = ujson.Obj("orderId" -> orderId, "customerId" -> w.customer, "orderTotal" -> money(amount),
                               "carrier" -> carrier(orderId), "dispatchedAt" -> now.toString)
          List(Out(Dispatched, orderId, emit("OrderDispatched", 1, now, data)))
        case _ =>
          waiting.update(orderId, w)
          Nil

  private def expire(now: Instant): Unit =
    val cutoff = now.minus(ttl)
    while waiting.headOption.exists(_._2.since.isBefore(cutoff)) do waiting.remove(waiting.head._1)

object Shipping:
  val Group      = "shipping-service"
  val Dlq        = "shipping.dlq"
  val Dispatched = "shipping.dispatched"
  val Reads      = List(Payments.Authorized, Inventory.Reserved)

  private val carriers = Vector("ups", "fedex", "usps", "dhl")
  def carrier(orderId: String): String = carriers(math.floorMod(orderId.hashCode, carriers.size))
