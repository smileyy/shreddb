# shreddb

## What is it?

Shreddb is a stateless (primarily*) column-oriented tool for indexing and querying tabular data. I consider it stateless 
because it has no "working memory". That is, there is no concept of a table being kept in memory to run queries 
against. It is designed to (efficiently) load from a file store (like S3 or some other cloud blob store, and also you 
can use a filesystem for local testing). It's called "shred" because it shreds the data into individual files for each 
column and only accesses the columns necessary to execute the query, kinda like a paper shredder shreds paper into strips.

(*) It doesn't _have_ to be column oriented. Someday I intend to write a row-based implementation to demonstrate how 
much better the columnar format is for queries.

### Why should I use it?
You shouldn't. This is a fun exercise in writing things from first principles. Go use Spark or something.

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

## Practical stuff

### How to run it
There are unit tests, run them. Note that you'll have to make the directory `/tmp/shreddb`. The tests currently leave 
garbage behind in that directory until we fix that (see Issue #3). 

### How to contribute
Grab an issue, cut a branch, make sure tests are appropriately updated, make your change, make sure the tests run (we 
don't have a build pipeline yet, see #14), submit a pull request. I should see it and get back to you.

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