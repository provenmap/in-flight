package rif.catalog

import scala.util.Try

/** What one event is worth, to whom, and where it sits on the map. */
final case class Priced(
    eventId: String,
    ref: EventRef,
    topic: String,
    district: String,
    value: BigDecimal,
    customer: String,
    orderId: String,
    redriveOf: Option[String]
)

/** Prices events by the paths the catalog declares. Nothing is guessed: an undeclared type or a missing path is an error. */
final class Pricing(catalog: Catalog):
  def price(event: CloudEvent): Either[String, Priced] =
    val where = s"${event.ref.show} (${event.id})"
    for
      spec     <- catalog.find(event.ref).toRight(s"$where is not in the catalog")
      value    <- spec.value.read(event.data).flatMap(Pricing.money).toRight(s"$where: no amount at '${spec.value.show}'")
      customer <- spec.customer.read(event.data).flatMap(_.strOpt).toRight(s"$where: no customer at '${spec.customer.show}'")
      order    <- spec.order.read(event.data).flatMap(_.strOpt).toRight(s"$where: no order at '${spec.order.show}'")
    yield Priced(event.id, event.ref, spec.topic, spec.district, value, customer, order, event.redriveOf)

  def price(bytes: Array[Byte]): Either[String, Priced] = CloudEvents.parse(bytes).flatMap(price)

object Pricing:
  lazy val default: Pricing = Pricing(Catalog.default)

  /** An amount, written as a decimal string or a JSON number. */
  def money(v: ujson.Value): Option[BigDecimal] =
    v.strOpt.flatMap(s => Try(BigDecimal(s)).toOption)
      .orElse(v.numOpt.map(BigDecimal(_)))
