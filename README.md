# shreddb

## What is it?
Shreddb is a stateless column-oriented(*) tool for indexing and querying/aggregating tabular data. I consider 
it stateless because it has no "working memory". That is, there is no concept of a table being kept in memory to run 
queries against. It is designed to (hopefully!) efficiently load from a blob store (like S3 or some other cloud store, 
and also you a filesystem for local testing). It's called "shred" because it shreds the data into individual files for 
each column and only accesses the columns necessary to execute the query, kinda like a paper shredder shreds paper into 
strips.

(*) It probably doesn't _have_ to be column oriented. Someday I intend to write a row-based implementation to 
demonstrate how much better the columnar format is for queries.

### Why should I use it?
You shouldn't. This is a fun exercise in writing things from first principles. Go use Spark or something.

### No, really, I want to use it! How do I use it?
Detailed usage can be found in the unit tests. See `SimpleCsvTestBase` for supported query operations and syntax. See
`StringColumnsFilesystemTest` and `TokenizedColumnsFilesystemTest` for different column implementations (see below
about tokenization).

But you want to read documentation, and not code?! Ok...there are three steps to using shreddb:

1. Configure shreddb
2. Shred the input data 
3. Query the table

#### Configuring shreddb
Implement `ShredConfiguration` to map storage system implementations to the names that will be stored in the table 
manifest (see more about manifests below).

The following example creates a configuration with a filesystem storage in it.

```scala
object MyConfiguration extends ShredConfiguration {
  override protected def getUnderlyingStorage(name: String, metadata: Map[String, String]): Storage = name match {
    case "file" => new FilesystemStorage(Path.of("/tmp/shreddb"))
  }
}

val shreddb = new Shreddb(MyConfiguration)
```

#### Shredding the input data
For this documentation, we'll consider a CSV file containing employee information such as name, title, and salary. In 
order to shred this data, we need to provide the following information*:
- The definition of the table's columns
- the name of the storage system (matching a value in our configuration)
- (optionally) any compression to be used

(*) There's a concept of a `TableFormat` but `ColumnsFormat` is the only option for that. Refer to the previous note 
about producing a row-oriented implementation for performance testing / no-really-I-just-want-to-iterate-a-CSV-file.

```scala
val input = new CsvInput(new InputStreamReader(Files.newInputStream(Path.of("employees.csv"), StandardCharsets.UTF_8)))
val columns = Seq(
  ColumnDefinition("name", StringColumnFormat())
  ColumnDefinition("title", TokenizedStringColumnFormat())
  ColumnDefinition("salary", DecimalColumn())
)
val tableDefinition = TableDefinition("employees", columns)

val shredManifest: ShredManifest = shreddb.shred(input, tableDefinition, "file", GzipCompression)
```

Shredding produces a `ShredManifest` which is then used to reconstruct the table when querying.

#### Querying the table
In order to run a query, we need a table:

```scala
val table = shreddb.getTable(shredManifest)
```

Note that because of that whole stateless thing, nothing has happened yet; the table is a pointer to a bunch of column
definitions and the files that contain those columns.

As alluded to above, queries are aggregations of data. The data we're aggregating is salary information, indexed by the
other fields in the data. Results are returned as a `ShredResultSet`: a collection of `ShredResultSetRow`s, each of 
which has two attributes: a `Seq` values of the fields that the aggregations were grouped by, and then a `Seq` of the 
aggregated values. Note that addressing columns in result sets is still a bit awkward. See Issue #18 for the plan to 
improve on this.

First, let's query for total payroll:
```scala
val results: ShredResultSet = table.query(select(Sum.of("salary")))
val row: ShredResultSetRow = results.iterator.next
val total: BigDecimal = row.values.head
```

Now, let's get the average salary by title:
```scala
val averageSalaryByTitle = table.query(select(Average.of("salary")).groupBy("title"))
```

Let's do some filtering and figure out the total comp for C-suite executives:
```scala
val averageExecutiveSalary = table.query(select(Sum.of("salary")).where("title".in("CEO", "CFO", "CIO", "COO", "CPO")))
```

Finally, let's get fancy and figure out how much the average Bob gets paid:
```scala
val bob = table.query(select(Average.of("salary")).where("name".suchThat { name => name.startsWith("Bob") }))
```

Note: there's some static import and implicit conversion magic that requires imports to make the literate query syntax
we've used work. You can see specifics in the unit tests previously referenced.

### How does it work?
Shreddb takes advantage of the columnar format by using a (not particularly complicated) algorithm that I'm calling
K-Way Consensus*. The basic idea is, given K arrays of objects: `a`, `b`, and `c`, and corresponding boolean-valued 
functions; `f`, `g`, and `h` that evaluates members of the corresponding array for truth, how do you find the 
intersection of all array indexes that evaluate to true? A naive approach is to evaluate each member of each array, 
find the appropriate indexes for each array, and intersect those sets together to come up with the final set of matching
indexes.

Now imagine that `f`, `g`, and `h` aren't (necessarily) cheap to execute -- think a string comparison. I thought about 
it a bit and figured out that you don't need to evaluate every element of every array; you can use information from one
column as you iterate it to figure out what is and is not interesting about other columns.

The algorithm starts by finding the first match in column `a`. Say that's at index 2. We had to call `f` for indexes
0, 1, and 2 to get there. When searching column `b`, we now know that indexes 0 and 1 don't matter, so we can fetch
that data but not call `g` to evaluate those positions; we only need to evaluate index 2. If 2 doesn't match for `h(b)`,
the algorithm continues to search `b` to find the next match. If that's index 7, we know we can skip indexes 0-6 for
`c` and 3-6 for `a`. When three matching indexes are found, the algorithm starts all over again. 

The (somewhat non-Scala**) implementation is found in `ColumnTable`. This algorithm probably isn't exactly novel or 
clever, but I'm happy I wrote it.

(*) h/t to the Indeed K-Way Merge interview question for the name
(**) I love my `while (!done)` loops...and footnotes apparently

### What's the whole tokenization thing about anyway?
A `TokenizedStringColumnFormat` column, as its name suggests, tokenizes the column values, replacing the field values
as stored in the column with integers, which are mapped to a lookup table. This is valuable in the case where we
expect to query fields with "enumerated" values.

Tokenization is valuable because my empirical experiences have shown two things are very important when running a 
stateless, blob store-backed database like shreddb:
1. Size of data streamed over the network matters
2. String comparisons are more expensive than integer comparisons

While compression like gzip helps with the first point, it does nothing to help with the second. Hopefully I can do 
some performance testing to back these claims up in the near future.

## Practical stuff

### How to run it
There are unit tests, run them. Note that you'll have to make the directory `/tmp/shreddb`. The tests currently leave 
garbage behind in that directory until we fix that (see Issue #3). 

### How to contribute
Grab an issue, cut a branch, make sure tests are appropriately updated, make your change, make sure the tests run (we 
don't have a build pipeline yet, see Issue #14), submit a pull request. I should see it and get back to you.

If you're feeling spicy, open an issue yourself; I'll triage it (IOW, let you know if I like the idea) and let you know.

### Rough style guide
#### Prefer explicit braced functional parameters
Like this:
```scala
foo.bar(a) { b => f(b) }
```

Yes, it's verbose. It reads easier to me than just parens or the use of `_`s

#### No wildcard imports
Yeah, no, I like to see what exactly is being imported. I make an exception for the import of implicits (which should 
be used sparingly anyway).

#### Optimize imports on changed classes
Nobody likes unused imports.

### scalafmt
There's a `.scalafmt` file that I don't strictly pay attention to / format with. So there's some run-on lines. Sorry!