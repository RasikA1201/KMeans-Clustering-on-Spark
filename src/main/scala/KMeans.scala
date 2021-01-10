/*
Name: Rasika Hedaoo
Student ID: 1001770527
*/

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object KMeans {

	type txtPoint=(Double,Double)						//declaration of type of points 
	var centroids: Array[txtPoint] = Array[txtPoint]()	//declaration of centroids
	def distance (p: txtPoint, pt: txtPoint): Double ={
		var distance = Math.sqrt ((p._1 - pt._1) * (p._1 - pt._1) + (p._2 - pt._2) * (p._2 - pt._2) ); //find distance from the given points
		distance
	}

	def main (args: Array[String]): Unit ={
		val sConf = new SparkConf().setAppName("KMeans")
		val sc = new SparkContext(sConf)

		val pointsValue = sc.textFile(args(0))
		var points = pointsValue.map(line => {val convrt = line.split(",")		//points from the file 
		(convrt(0).toDouble,convrt(1).toDouble)})

		val CentroidValue = sc.textFile(args(1))
		centroids = CentroidValue.map( line => { val convrts = line.split(",")		//centroids from the file 
		(convrts(0).toDouble,convrts(1).toDouble)}).collect

	/* for ( i <- 1 to 5 ) {
       val cs = sc.broadcast(centroids)
       centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
                         .groupByKey().map { /* ... calculate a new centroid ... */ }
    } */

    for( i <- 1 to 5){								//the process of finding new centroids from previous centroids using KMeans must be repeated 5 times
		val cs = sc.broadcast(centroids)			//broadcast the centroids to worker nodes using the Spark broadcast method
		centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }		//distance(x,y) calculates the distance between two points x and y
        .groupByKey().map { case(c,newPoint)=>		//calculate a new centroid

        var calculateX = 0.0						//declare variable for adding X points
        var calculateY = 0.0						//declare variable for adding Y points
		var nPoints = 0.0							//declare variable for counting number of points

        for(pointing <- newPoint) {
           calculateX += pointing._1				//claculating sum of X points
           calculateY += pointing._2				//claculating sum of Y points
		   nPoints += 1								//claculating total points in X and Y
        }
        var centroidMeanX = calculateX/nPoints		//claculating mean of X
        var centroidMeanY = calculateY/nPoints		//claculating mean of Y
        (centroidMeanX, centroidMeanY)
		}.collect
    }

	centroids.foreach(println)						//printing all the data for small and large file
    }
}