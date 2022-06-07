package io.datapotion.sparky

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

import java.security.MessageDigest

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("spark session")
      .getOrCreate()
  }

}

object Main extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
      (2, "Rose", 1, "2010", "20", "M", 4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1)
    )
    val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
      "emp_dept_id", "gender", "salary")

    import spark.sqlContext.implicits._
    val empDF = emp.toDF(empColumns: _*)
    empDF.show(false)

    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40)
    )

    val deptColumns = Seq("dept_name", "dept_id")
    val deptDF = dept.toDF(deptColumns: _*)

    val joined = empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner")
    val optPlan = joined.queryExecution.optimizedPlan
    val anPlan = joined.queryExecution.analyzed

    val list1 =  visualize(anPlan, Nil, "")
    val list2 = parseToMermaid(list1)
    list2.foreach(println)

    println("============================")

    val list3 =  visualize(optPlan, Nil, "")
    val list4 = parseToMermaid(list3)
    list4.foreach(println)

  }

  private def parseToMermaid(list: List[(Node, Node)]) = {
    list.distinct.map(x => s" ${x._1.hash}[${x._1.name}] --> ${x._2.hash}[${x._2.name}]")
  }

  case class Node(name: String, info: String, hash: String){
    def this(name: String, info: String) = {
      this(name, info, MessageDigest.getInstance("MD5").digest(s"$name $info".getBytes).mkString)
    }
 }

  def createNode(source: String): Node = {
  val split = source.split(" ")
    new Node(split.head, split.tail.mkString)
  }

  def GenerateIds(): List[Char] = {
    (65 to 90).map(x => x.toChar).toList
  }

  /*
 A[Join]
 A --> B[Project]
 B --> C[Local Relation]
 A --> D[Project]
 D --> E[Local Relation]
   */

  // TODO please tidy up this bonanza at some point
  def visualize(plan: LogicalPlan, list: List[(Node, Node)], parent: String): List[(Node, Node)] = {
    if (parent.isEmpty) {
      if(plan.children.isEmpty)
        list
      else
        (plan.children.map(child => visualize(child, list, plan.verboseStringWithSuffix(25))) :+ list ).flatten.toList
    } else {
      val x2 = (createNode(parent),createNode(plan.verboseStringWithSuffix(25))) :: list
      if(plan.children.isEmpty)
        x2
      else
        (plan.children.map(child => visualize(child, x2, plan.verboseStringWithSuffix(25))) :+ x2 ).flatten.toList
    }
  }
}
