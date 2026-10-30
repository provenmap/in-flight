package rif.simulator

import rif.catalog.CloudEvent

import java.time.Instant
import scala.util.Random

final case class Product(sku: String, price: BigDecimal)

/**
 * The orders team: synthetic shoppers placing orders. Version 1 writes `total` as a decimal string; version 2,
 * the breaking change, writes it as `{ amount, currency }`.
 */
object Orders:
  val Topic     = "orders.placed"
  val Customers = 2000
  val Currency  = "USD"

  /** A shop's range, cheap things far more often than dear ones. Weight, then product. */
  val range: List[(Int, Product)] = List(
    30 -> Product("SKU-SOCKS-3PK", BigDecimal("12.99")),
    28 -> Product("SKU-MUG-CERAMIC", BigDecimal("16.50")),
    25 -> Product("SKU-TSHIRT", BigDecimal("24.00")),
    22 -> Product("SKU-PHONE-CASE", BigDecimal("29.95")),
    18 -> Product("SKU-BOOK-HC", BigDecimal("34.99")),
    15 -> Product("SKU-SNEAKERS", BigDecimal("89.00")),
    12 -> Product("SKU-BACKPACK", BigDecimal("119.00")),
    10 -> Product("SKU-HEADPHONES", BigDecimal("149.00")),
    6  -> Product("SKU-SMARTWATCH", BigDecimal("329.00")),
    4  -> Product("SKU-ESPRESSO", BigDecimal("549.00")),
    2  -> Product("SKU-LAPTOP-14", BigDecimal("1299.00")),
    1  -> Product("SKU-TV-65", BigDecimal("1899.00"))
  )
  private val totalWeight = range.map(_._1).sum

  def customerId(i: Int): String = f"cus-$i%04d"

  def pick(rng: Random): Product =
    var n = rng.nextInt(totalWeight)
    range.find { (w, _) => n -= w; n < 0 }.get._2

  /** One to four distinct lines, usually one, mostly one of each; now and then a business buys ten of something cheap. */
  def basket(rng: Random): List[(Product, Int)] =
    val r     = rng.nextDouble()
    val lines = if r < 0.5 then 1 else if r < 0.8 then 2 else if r < 0.95 then 3 else 4
    List.fill(lines)(pick(rng)).distinct.map { p =>
      val q   = rng.nextDouble()
      val qty = if q < 0.03 && p.price < 50 then 10 else if q < 0.2 then 2 + rng.nextInt(2) else 1
      (p, qty)
    }

  def placed(emit: Emitter, rng: Random, version: Int, at: Instant): Out =
    val orderId  = f"ord-${rng.nextLong() & 0xffffffffffL}%010x"
    val customer = customerId(1 + rng.nextInt(Customers))
    val lines    = basket(rng)
    val total    = lines.map((p, q) => p.price * q).sum
    val items    = ujson.Arr.from(lines.map((p, q) => ujson.Obj("sku" -> p.sku, "qty" -> q, "price" -> money(p.price))))
    val data = version match
      case 1 => ujson.Obj("orderId" -> orderId, "customerId" -> customer, "total" -> money(total), "currency" -> Currency,
                          "items" -> items, "placedAt" -> at.toString)
      case _ => ujson.Obj("orderId" -> orderId, "customerId" -> customer,
                          "total" -> ujson.Obj("amount" -> money(total), "currency" -> Currency),
                          "items" -> items, "placedAt" -> at.toString)
    Out(Topic, orderId, emit("OrderPlaced", version, at, data))

  def isPlaced(e: CloudEvent): Boolean = e.eventType == "OrderPlaced"
