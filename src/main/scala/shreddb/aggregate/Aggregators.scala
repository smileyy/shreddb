package shreddb.aggregate

trait Aggregator {
  def add(newValue: BigDecimal): Unit
  def value: BigDecimal
}

trait AggregatorFactory {
  def newAggregator(): Aggregator
}

class SumAggregator(var value: BigDecimal) extends Aggregator {
  def add(newValue: BigDecimal): Unit = {
    value = newValue + value
  }
}

object SumAggregatorFactory extends AggregatorFactory {
  override def newAggregator(): Aggregator = new SumAggregator(0)
}