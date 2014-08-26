import org.example.{RowSelection, Dimension, AggregationOperation, Metric}
val metric1 = Metric("m1", AggregationOperation.Sum)
val metric2 = Metric("m2", AggregationOperation.Average)
val dimension1 = Dimension("d1")
val dimension2 = Dimension("d2")
val selection = RowSelection(
  Seq(metric1, metric2),
  Seq(dimension1, dimension2),
  // metrics rows
  Seq(IndexedSeq(1.0, 3.0), IndexedSeq(2.0, 9.0)),
  // dimensions rows
  Seq(IndexedSeq("dimVal11", "dimVal12"), IndexedSeq("dimVal21", "dimVal22"))
)

val f: (String => Boolean) = dimVal => (dimVal == "dimVal12")
val filtered = selection.filter(dimension2, f)
