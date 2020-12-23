package dataretrieve

import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.Query
import twitter4j.GeoLocation

import twitter4j.conf.ConfigurationBuilder
import twitter4j.QueryResult
import scala.collection.JavaConversions._
import scala.util.control.Breaks._

import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.ListBuffer

import org.rogach.scallop._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
// https://dzone.com/articles/csv-file-writer-using-scala
// csv

// https://www.socialseer.com/twitter-programming-in-java-with-twitter4j/how-to-retrieve-more-than-100-tweets-with-the-twitter-api-and-twitter4j/
// usefull link

// http://twitter4j.org/javadoc/twitter4j/Status.html
// twitter4j API
class HistoricalDataConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(tweets_per_query, max_query, filter,output,max_id)
  val tweets_per_query = opt[Int](descr = "tweets per query/request (max 100)", required = false, default = Some(100))
  val max_query = opt[Int](descr = "max number of queries/requests",  required = false, default = Some(2))
  val filter = opt[String](descr = "twitter search filter",  required = false, default = Some(""))
  val output = opt[String](descr = "output path",  required = false, default = Some("data/twitter_history"))
  val max_id = opt[Long](descr = "mx tweets id",  required = false, default = Some(-1l))
  verify()
}
object TwitterHistoricalData {

    def cleanText(text: String) : String = {
        var final_text = text.replace("\n", " ")
        final_text = final_text.replace("\t", " ")
		final_text
    }
    def main(argv : Array[String]) {

        val args = new HistoricalDataConf(argv)

        val tweets_per_query = args.tweets_per_query()
        val max_queries = args.max_query()
        val filter = args.filter()
        var maxID = args.max_id()
        // var maxID = 1333222199960989701l
        
        var total_tweets = 0

        var cur_timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
        
        val consumerKey = "YOURACCESSTOKEN"
        val consumerSecret = "YOURACCESSSECRET"
        val accessToken = "YOURCONSUMERKEY"
        val accessTokenSecret = "YOURCONSUMERSECRET"

        // value for saving csv files
        val outputFile = new BufferedWriter(new FileWriter(args.output()+cur_timestamp+".csv"))
        val csvWriter = new CSVWriter(outputFile)
        val csvFields = Array("id","date","user","text")
        var listOfRecords = new ListBuffer[Array[String]]()
        listOfRecords += csvFields

        // (1) config work to create a twitter object
        val cb = new ConfigurationBuilder()
        cb.setDebugEnabled(true)
        // comments out due to version issue
        .setTweetModeExtended(true)
        .setOAuthConsumerKey(consumerKey)
        .setOAuthConsumerSecret(consumerSecret)
        .setOAuthAccessToken(accessToken)
        .setOAuthAccessTokenSecret(accessTokenSecret)
        // able to get the full text

        val tf = new TwitterFactory(cb.build())
        val twitter = tf.getInstance()

        // check how many limits left
        val rateLimitStatus = twitter.getRateLimitStatus("search")
        var searchTweetsRateLimit = rateLimitStatus.get("/search/tweets")


        //	Always nice to see these things when debugging code...
        printf("You have %d calls remaining out of %d, Limit resets in %d seconds\n",
                                searchTweetsRateLimit.getRemaining,
                                searchTweetsRateLimit.getLimit,
                                searchTweetsRateLimit.getSecondsUntilReset)


        // (3) search tweets for the location near waterloo
        breakable {
            for (queryNumber <- 1 to max_queries){
                
                println("Starting the query....",queryNumber)
                
                //	Do we need to delay because we've already hit our rate limits?
                if (searchTweetsRateLimit.getRemaining == 0)
                {
                    printf("!!! Sleeping for %d seconds due to rate limits\n", searchTweetsRateLimit.getSecondsUntilReset)
                    Thread.sleep((searchTweetsRateLimit.getSecondsUntilReset+2) * 1000l)
                } 

                var query = new Query(filter)
                query.setCount(tweets_per_query)  
                // comment out due to version issue
                // original: Query.Unit.km
                // instead of: "km"

                // Waterloo
                // query.setGeoCode(new GeoLocation(43.455607,-80.540809),25, Query.Unit.km)
                // Toronto
                query.setGeoCode(new GeoLocation(43.64654421235419, -79.46356884810994),30, Query.Unit.km)
                
                // restricts tweets to give only English
                query.setLang("en")

                // query.setResultType(Query.POPULAR)         
                
                if (maxID != -1)
                {
                    query.setMaxId(maxID - 1)
                }


                var result = twitter.search(query)
                if (result.getTweets.size == 0)
                {
                    break
                }

                println("Showing serach results: "+result.getTweets.size)
                for (status <- result.getTweets) {
                    total_tweets += 1
                    var tweets_id = status.getId
                    var tweets_text = ""
                    if (status.getRetweetedStatus() != null) {
                        tweets_text = cleanText(status.getRetweetedStatus().getText());
                    } else if (status.getRetweetedStatus() == null) {
                        tweets_text = cleanText(status.getText)
                    }       
                     
                    if (maxID == -1 || tweets_id < maxID)
                    {
                        maxID = tweets_id
                    }
                
                    listOfRecords += Array(tweets_id.toString,status.getCreatedAt.toString,status.getUser.getScreenName,tweets_text )
                    println(tweets_id+","+ status.getCreatedAt + ","+ status.getUser.getScreenName + "," + tweets_text)
                
                }

                searchTweetsRateLimit = result.getRateLimitStatus
            }
        }

        // (2) use the twitter object to get your friend's timeline
        // val statuses = twitter.getFriendsTimeline()
        // System.out.println("Showing friends timeline.")
        // val it = statuses.iterator()
        // while (it.hasNext()) {
        //   val status = it.next()
        //   println(status.getUser().getName() + ":" +
        //           status.getText());
        // }
        csvWriter.writeAll(listOfRecords.toList)
        outputFile.close()
  }
}
