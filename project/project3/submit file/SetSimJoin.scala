package comp9313.proj3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SetSimJoin {
  def main(args: Array[String]): Unit = {
    val data_path = args(0)
    val outputPath = args(1)
    val t = args(2).toDouble
    //
    //    val data_path = "9313data/project3/unsorted.txt"
    //    val t = 0.8
    //    val outputPath = "9313data/project3/unsorted_result"
    val conf: SparkConf = new SparkConf().setAppName("project3")
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.textFile(data_path)

    // calculate frequency, stored in hashmap and then broadcast it .
    val order_token: Map[String, Int] = data.flatMap(
      x => x.split(" ").drop(1).map(word => (word, 1)))
      .reduceByKey(_ + _).sortBy(x => x._2).collect().toMap
    val TOKENS: Broadcast[Map[String, Int]] = sc.broadcast(order_token)

    // use prefix filtering to map
    // emit (token, id, items)
    val map_result: RDD[(String, (String, Array[String]))] = data.flatMap(line => {
      val line_item: Array[String] = line.split(" ")
      val id: String = line_item.head
      val items: Array[String] = line_item.drop(1)
      //calculate prefix length
      val prefix_length: Int = math.min(items.length - math.ceil(items.length * t).toInt + 1, items.length)
      //prefix filtering, only emit prefix tokens
      val items_sort_by_token = items.sortBy(item => (TOKENS.value.get(item), item))

      for (i <- 0 until prefix_length) yield (items_sort_by_token(i), (id, items))
    })


    // group by token, for each pair of item sets, compute similarity score, if score >= threshold, yield the pair
    // return (rid1, rid2, similarity)
    val similarity_result: RDD[(Long, Long, Double)] = map_result.groupByKey().flatMap(x => {
      //cast iterable to list
      val item_list = x._2.toList

      for {
        i <- 0 until item_list.length - 1
        j <- i + 1 until item_list.length
        // compute similarity score
        intersection_set = item_list(i)._2.intersect(item_list(j)._2)
        similar_score: Double = intersection_set.length / (item_list(i)._2.length + item_list(j)._2.length - intersection_set.length).toDouble
        if similar_score >= t
      } yield {
        // make sure the the smaller rid prior to the larger rid, make sure working in multiple nodes
        if (item_list(i)._1.toLong < item_list(j)._1.toLong)
          (item_list(i)._1.toLong, item_list(j)._1.toLong, similar_score)
        else
          (item_list(j)._1.toLong, item_list(i)._1.toLong, similar_score)
      }
    })


    // make it distinct
    // sort output
    // format output
    val sorted_result = similarity_result.distinct().sortBy(x => (x._1, x._2)).map(x => "(" + x._1 + "," + x._2 + ")" + "\t" + x._3)


    sorted_result.saveAsTextFile(outputPath)

    sc.stop()


  }

}
