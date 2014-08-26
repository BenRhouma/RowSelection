package org.example

import org.example.AggregationOperation.AggregationOperation


object AggregationOperation extends Enumeration {
  type AggregationOperation = Value
  val Sum, Average, Min, Max, Count = Value
}

case class Metric(id: String, aggregationOperation: AggregationOperation)

case class Dimension(id: String)


/**
 * Think of a RowSelection as a typed SQL table. We use this selection to
 * modelize OLAP cubes.
 *
 * Headers are the names of each column, with an additionnal information about
 * wether we are dealing with a dimension or a metric.
 *
 * Rows are double dimensions arrays.
 * Each row is viewed as 2 concatened sequence: one of strings for dimension values, the others of numerics.
 *
 * Ex:
 * ["ga:origin", "ga:browser"] ["ga:visits", "ga:timeOnSite"]
 * ["France", "Chrome"] [20.0, 345.5]
 * Assumptions :
 * ["France", "Chrome"] [20.0, 345.5]
 * ["France", "Chrome"] [15.0, 10.5]
 * ["France", "Chrome"] [22.0, 345.5]
 * ["France", "Firefox"] [7.0, 290.5]
 * ["France", "IE8"] [1.0, 50.5]
 * ["Tunisia", "Chrome"] [25.0, 345.5]
 * ["Tunisia", "Firefox"] [17.0, 290.5]
 * ["Tunisia", "IE8"] [1.0, 20.5]
 */

case class RowSelection(
                         metricHeaders: Seq[Metric],
                         dimensionHeaders: Seq[Dimension],
                         metricRows: Seq[IndexedSeq[Double]],
                         dimensionRows: Seq[IndexedSeq[String]]) {


  def take(n: Int): RowSelection = {
    RowSelection(metricHeaders, dimensionHeaders, metricRows take n, dimensionRows take n)
  }

  def ++(other: RowSelection): RowSelection = {
    this.copy(dimensionRows = this.dimensionRows ++ other.dimensionRows, metricRows = this.metricRows ++ other.metricRows)
  }

  def filter(col: Dimension, f: (String) => Boolean): RowSelection = {
    val index = this.dimensionHeaders.indexOf(col)
    var mt : Seq[IndexedSeq[Double]] = Seq.empty
    val x  = this.dimensionRows.filter{
      a : IndexedSeq[String] =>
        if(f(a(index))){
          mt ++= this.metricRows.lift(this.dimensionRows.indexOf(a))
           true
        }else
          false
    }
    this.copy(dimensionRows = x , metricRows = mt)
  }


  def filter(col: Seq[Dimension], values: IndexedSeq[String]): RowSelection = {

    val res = this.dimensionRows zip this.metricRows
    val filteredList = res.filter{
      case (d,m)=>
       var verif : Boolean = true
        for(dimension<-col){
          // dimension index within the Seq
          val index = this.dimensionHeaders.indexOf(dimension)
          if(!d(index).equals(values(index)))
            verif = false
        }
      verif
    }

    val mt : Seq[IndexedSeq[Double]]=for(e  <- filteredList)
      yield  e._2
    val md : Seq[IndexedSeq[String]] = for(e<- filteredList)
      yield e._1

    this.copy(dimensionRows = md , metricRows = mt)
  }


  def groupByColumns(cols: Seq[Dimension]): RowSelection = {
    var metricRowList : Seq[IndexedSeq[Double]] = Seq.empty[IndexedSeq[Double]]
    for(dmr : IndexedSeq[String] <- this.dimensionRows.distinct){

      var metricRow : Seq[IndexedSeq[Double]] = Seq.empty
      val metrics = this.filter(cols , dmr).metricRows
      var ls : IndexedSeq[Double] = IndexedSeq.empty
      this.metricHeaders.zipWithIndex.foreach(
        (mh) => mh._1.aggregationOperation match {
          case AggregationOperation.Sum => {
            ls  = ls :+  (for (e <- metrics) yield e(mh._2)).sum
          }
          case AggregationOperation.Max => {
            ls  = ls :+  (for (e <- metrics) yield e(mh._2)).max
          }
          case AggregationOperation.Min => {
            ls  = ls :+ (for (e <- metrics) yield e(mh._2)).min
          }
          case AggregationOperation.Count => {
            ls  = ls :+ metrics.size.asInstanceOf[Double]
          }
          case AggregationOperation.Average=> {
            val values = for(e <- metrics)yield e(mh._2)
            ls  = ls :+ (values.sum / values.size)
          }
        }
      )
      metricRow = metricRow :+ ls
      metricRowList = metricRowList ++  metricRow
    }

    this.copy(dimensionRows=this.dimensionRows.distinct, metricRows = metricRowList)
  }


}