package shreddb.input.csv

import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}
import shreddb.input.{Input, InputRow}

import java.io.Reader
import scala.jdk.CollectionConverters.*

class CsvInput(reader: Reader, format: CSVFormat = CSVFormat.DEFAULT) extends Input {
  private val csv: CSVParser = format.builder().setHeader().setSkipHeaderRecord(true).get().parse(reader)

  val columnNames: Seq[String] = csv.getHeaderNames.asScala.toSeq

  override def iterator: Iterator[InputRow] = {
    csv.iterator().asScala.map { row =>
      new CsvInputRow(row)
    }
  }
}

class CsvInputRow(record: CSVRecord) extends InputRow {
  override def getValue(idx: Int): String = record.get(idx)
  override def getValue(field: String): String = record.get(field)
}
