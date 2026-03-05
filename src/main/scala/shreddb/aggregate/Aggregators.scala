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

class AverageAggregator(var sum: BigDecimal, var count: Long) extends Aggregator {
  override def add(newValue: BigDecimal): Unit = {
    sum = sum + newValue
    count = count + 1
  }

  override def value: BigDecimal = sum / count
}

object AverageAggregatorFactory extends AggregatorFactory {
  override def newAggregator(): Aggregator = new AverageAggregator(0, 0)
}

class TransformingAggregator(delegate: Aggregator, before: BigDecimal => BigDecimal) extends Aggregator {
  override def add(newValue: BigDecimal): Unit = delegate.add(before(newValue))
  override def value: BigDecimal = delegate.value
}
class TransformingAggregatorFactory(delegate: AggregatorFactory, before: BigDecimal => BigDecimal) extends AggregatorFactory {
  override def newAggregator(): Aggregator = new TransformingAggregator(delegate.newAggregator(), before)
}