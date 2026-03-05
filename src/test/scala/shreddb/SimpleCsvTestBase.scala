package shreddb

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.is
import org.junit.jupiter.api.Test
import shreddb.input.Input
import shreddb.input.csv.CsvInput

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets

trait SimpleCsvTestBase {
  def request: ShredRequest

  def input: Input = new CsvInput(
    new InputStreamReader(classOf[SimpleCsvTestBase].getResourceAsStream("simple-test-data.csv"), StandardCharsets.UTF_8)
  )

  @Test
  def simple(): Unit = {
    val shred = new ShredDb(TestConfiguration)

    val mf = shred.shred("simple-csv-test", request)

    val table = shred.getTable(mf)
    assertThat(table.name, is("simple-csv-test"))
    assertThat(table.columnNames, is(Seq("a", "b", "c", "tag:user", "qty")))

    val ungroupedResultSet = table.query(
      ShredQuery(
        select = Seq(Sum("qty")),
        where = Seq(
          In("a", Set("A", "C")),
          Is("c", "SQS")
        ),
        groupBy = Seq.empty
      )
    )

    assertThat(ungroupedResultSet.size, is(1))
    assertThat(ungroupedResultSet.rows.head.group, is(Seq.empty))
    assertThat(ungroupedResultSet.rows.head.values, is(Seq(BigDecimal(16))))

    val groupedResultSet = table.query(
      ShredQuery(
        select = Seq(Sum("qty")),
        where = Seq(
          In("a", Set("A", "C")),
          Is("c", "SQS")
        ),
        groupBy = Seq("a")
      )
    )

    assertThat(groupedResultSet.size, is(2))
    val groupedResultSetIterator = groupedResultSet.iterator
    val groupedRow1 = groupedResultSetIterator.next()
    assertThat(groupedRow1.group, is(Seq("A")))
    assertThat(groupedRow1.values, is (Seq(BigDecimal(1))))
    val groupedRow2 = groupedResultSetIterator.next()
    assertThat(groupedRow2.group, is(Seq("C")))
    assertThat(groupedRow2.values, is (Seq(BigDecimal(15))))

    val averagedResultSet = table.query(
      ShredQuery(
        select = Seq(Average("qty")),
        where = Seq(
          In("a", Set("A", "C")),
          Is("c", "SQS")
        ),
        groupBy = Seq("a")
      )
    )

    assertThat(averagedResultSet.size, is(2))
    val averagedResultSetIterator = averagedResultSet.iterator
    val averagedRow1 = averagedResultSetIterator.next()
    assertThat(averagedRow1.group, is(Seq("A")))
    assertThat(averagedRow1.values, is(Seq(BigDecimal(1))))
    val averagedRow2 = averagedResultSetIterator.next()
    assertThat(averagedRow2.group, is(Seq("C")))
    assertThat(averagedRow2.values, is(Seq(BigDecimal("7.5"))))
  }
}