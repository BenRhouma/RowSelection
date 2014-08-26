package org.example

import org.scalatest.{Matchers, FunSpec}


/**
 * Created by Zied on 26/08/2014.
 */

class RowSelectionSpec extends FunSpec with Matchers {

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


  describe("RowSelection") {

    it ("take the n first rows") {
      val taken = selection.take(1)
      taken.metricRows should have size(1)
      taken.dimensionRows should have size(1)
    }

    it ("add a selection to another selection" ){
      val other = selection
      val added = selection ++ other
      added.metricHeaders should equal(selection.metricHeaders)
      added.dimensionHeaders should equal(selection.dimensionHeaders)
      added.metricRows should equal(Seq(IndexedSeq(1.0, 3.0), IndexedSeq(2.0, 9.0), IndexedSeq(1.0, 3.0), IndexedSeq(2.0, 9.0)))
      added.dimensionRows should equal(Seq(IndexedSeq("dimVal11", "dimVal12"), IndexedSeq("dimVal21", "dimVal22"), IndexedSeq("dimVal11", "dimVal12"), IndexedSeq("dimVal21", "dimVal22")))
    }

    /**
     * ["ga:origin", "ga:browser"] ["ga:visits", "ga:timeOnSite"]
     * ["France", "Chrome"] [20.0, 345.5]
    */

    it ( "filter the selection" ) {
      val f: (String => Boolean) = dimVal => (dimVal == "dimVal11")
      val filtered = selection.filter(dimension1, f)
      filtered.metricRows should equal(Seq(IndexedSeq(1.0, 3.0)))
      filtered.dimensionRows should equal(Seq(IndexedSeq("dimVal11", "dimVal12")))
    }

    it ( "group by columns" ) {
      val s = selection.copy(dimensionRows = Seq(IndexedSeq("dimVal1", "dimVal2"), IndexedSeq("dimVal1", "dimVal2")))
      val grouped = s.groupByColumns(Seq(dimension1, dimension2))
      grouped.dimensionRows should equal(Seq(IndexedSeq("dimVal1", "dimVal2")))
      grouped.metricRows should equal(Seq(IndexedSeq(3.0, 6.0)))
    }


  }

}