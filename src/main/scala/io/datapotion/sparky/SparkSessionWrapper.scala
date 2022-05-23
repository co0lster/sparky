package io.datapotion.sparky

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
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

    visualize(anPlan)
    println("============================")
    visualize(optPlan)


  }

  // TODO create recursive method that creates mermaid graph
  /*
 A[Join]
 A --> B[Project]
 B --> C[Local Relation]
 A --> D[Project]
 D --> E[Local Relation]
   */

  def visualize(plan: LogicalPlan): Unit = {
    println(plan.verboseStringWithSuffix(25))
    plan.children.foreach(child => println(child.verboseStringWithSuffix(25)))
  }
}
