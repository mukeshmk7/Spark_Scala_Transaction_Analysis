import org.apache.spark.SparkContext;

import org.apache.spark.SparkConf;

object transaction {

  def main(args:Array[String]) {

    val sparkConf = new SparkConf().setAppName("Transaction").setMaster("local")

    val sc = new SparkContext(sparkConf)


/* Use case 1 -- Find the average cash and credit amount spent across all the transactions */
   

    val tran = sc.textFile("/home/hadoop/transactions").map(a=>a.split(","))

    /*persisting this RDD as it will be used as the base for all other use cases */

    tran.persist()

    val no_of_credits=tran.filter(x=>x.contains("credit")).count()
 
    val no_of_cash =tran.filter(x=>x.contains("cash")).count()


    /*Finding the sum of cash and credit payments overall */

    val tot_amt_credit=tran.map(x=>(x(3).toInt,x(6))).filter(x=>x._2.equals("credit")).map(x=>x._1).sum().toInt

    val tot_amt_cash=tran.map(x=>(x(3).toInt,x(6))).filter(x=>x._2.equals("cash")).map(x=>x._1).sum().toInt

    val avg_credit= (tot_amt_credit/no_of_credits)

    val avg_cash = (tot_amt_cash/no_of_cash)

    val avg_credit_header = sc.parallelize(Seq("Credit average"))

    val avg_cash_header = sc.parallelize(Seq("Cash average"))

    
    /* Adding the text as credit average and cash average along with the output rdd */

    val credit_fnl = avg_credit_header.zip(sc.parallelize(Seq(avg_credit.toString)))

    val cash_fnl = avg_cash_header.zip(sc.parallelize(Seq(avg_cash.toString)))

    credit_fnl.saveAsTextFile("/home/hadoop/Learnbay/Spark/avg_credit")

    cash_fnl.saveAsTextFile("/home/hadoop/Learnbay/Spark/avg_cash")


/*Find out the month of the year 2013 where most of the transactions happened*/


   val year_2013_tran = tran.filter(x=>x(1).contains("2013"))
   
   val cnt_month = year_2013_tran.map(x=>(x(1).substring(0,2),x(3).toInt)).reduceByKey(_+_).sortBy(x=>x._2,false).first()

   sc.parallelize(List(cnt_month).toList).saveAsTextFile("/home/hadoop/Learnbay/Spark/tot_cnt_month")

    
/* Find out the city where the transactions happened with the least amount */


   val leastcity = tran.map(x=>(x(5),x(3).toInt)).reduceByKey(_+_).sortBy(x=>x._2).first()

   sc.parallelize(Seq(leastcity.toString)).saveAsTextFile("/home/hadoop/Learnbay/Spark/least_city")


/* Find out the top 2 product category which has the maximum sales */

   val top2product = tran.map(x=>(x(4),x(3).toInt)).reduceByKey(_+_).sortBy(x=>x._2,false).take(2)

   sc.parallelize(top2product.toList).saveAsTextFile("/home/hadoop/Learnbay/Spark/top2product")
   
}

}


 
