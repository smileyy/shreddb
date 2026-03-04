package shreddb

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.is
import org.junit.jupiter.api.Test
import shreddb.column.{ColumnDefinition, DecimalColumnFormat, EnumeratedColumnFormat, TokenizedEnumeration}
import shreddb.input.csv.CsvInput
import shreddb.storage.{Filesystem, FilesystemStorage, NoCompression, Storage, StorageSystem}
import shreddb.{ColumnsFormat, In, Is, ShredConfiguration, ShredDb, ShredQuery, Sum}

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.Path

class ColumnFilesystemTest {
  @Test
  def simple(): Unit = {
    val shred = new ShredDb(ColumnsTestConfiguration)

    val input = new CsvInput(
      new InputStreamReader(getClass.getResourceAsStream("simple-test-data.csv"), StandardCharsets.UTF_8)
    )

    val columns = Seq(
      ColumnDefinition("a", EnumeratedColumnFormat(TokenizedEnumeration)),
      ColumnDefinition("b", EnumeratedColumnFormat(TokenizedEnumeration)),
      ColumnDefinition("c", EnumeratedColumnFormat(TokenizedEnumeration)),
      ColumnDefinition("qty", DecimalColumnFormat)
    )

    val mf = shred.shred(
      "simple-test",
      input,
      Filesystem,
      columns,
      Some(EnumeratedColumnFormat(TokenizedEnumeration)),
      NoCompression,
      ColumnsFormat
    )
    val table = shred.getTable(mf)

    assertThat(table.name, is("simple-test"))
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
  }

}

object ColumnsTestConfiguration extends ShredConfiguration {
  override def getStorageSystem(system: StorageSystem, metadata: Map[String, String]): Storage = system match {
    case Filesystem =>
      new FilesystemStorage(Path.of("/tmp/shreddb"))
  }
}
