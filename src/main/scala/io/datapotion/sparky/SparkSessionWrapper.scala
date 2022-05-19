package io.datapotion.sparky

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}

class Main extends SparkSessionWrapper {

import spark.implicits._

val myDF = Seq(1,2,3).toDF

val plan = myDF.queryExecution.optimizedPlan

val children = plan.children

val innerChildren = plan.innerChildren

val verboseString = plan.verboseString(9999)

val verboseStringWithSuffix = plan.verboseStringWithSuffix(9999)

val verboseStringWithOperatorId = plan.verboseStringWithOperatorId()

val simpleString = plan.simpleString(9999)

val simipleStringWithNodeId = plan.simpleStringWithNodeId()


def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    append("   " * indent)
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        append(if (isLast) "   " else ":  ")
      }
      append(if (lastChildren.last) "+- " else ":- ")
    }

    val str = if (verbose) {
      if (addSuffix) verboseStringWithSuffix(maxFields) else verboseString(maxFields)
    } else {
      if (printNodeId) {
        plan.simpleStringWithNodeId()
      } else {
        simpleString(maxFields)
      }
    }
    append(prefix)
    append(str)
    append("\n")

    if (innerChildren.nonEmpty) {
      innerChildren.init.foreach(_.generateTreeString(
        depth + 2, lastChildren :+ children.isEmpty :+ false, append, verbose,
        addSuffix = addSuffix, maxFields = maxFields, printNodeId = printNodeId, indent = indent))
      innerChildren.last.generateTreeString(
        depth + 2, lastChildren :+ children.isEmpty :+ true, append, verbose,
        addSuffix = addSuffix, maxFields = maxFields, printNodeId = printNodeId, indent = indent)
    }

    if (children.nonEmpty) {
      children.init.foreach(_.generateTreeString(
        depth + 1, lastChildren :+ false, append, verbose, prefix, addSuffix,
        maxFields, printNodeId = printNodeId, indent = indent)
      )
      children.last.generateTreeString(
        depth + 1, lastChildren :+ true, append, verbose, prefix,
        addSuffix, maxFields, printNodeId = printNodeId, indent = indent)
    }
  }
}