import cascading.operation.Buffer
import com.twitter.scalding._
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.json4s._
import org.json4s.native._ //JsonMethods._
import org.joda.time.DateTime

object JSONMailParser {
    def main(args: Array[String]) {
        ToolRunner.run(new Configuration, new Tool, args)
    }
}


class JSONMailParser(args:Args) extends Job(args)
{
  implicit val formats = org.json4s.DefaultFormats // required for extracting object out of JSON

  val fieldSchema = ('success,'from,'to,'subject,'date,'attachments,'labels,'body)
  val finalSchema = ('from,'to,'subject,'date,'attachments,'labels,'comment,'body)

  val parsedPipe=TextLine(args("input"))
    .addTrap(Csv("resources/errors"))
    .map('line->fieldSchema)
  {
    line:String => {
        val parsed=JsonMethods.parse(line).extract[mailJSON] // parse JSON and then "unmarshall" into mailJSON case class
        // See coments for mailJSON case class regarding why these fields aren't wrapped in Option[T]
      (1,scrubAddresses(parsed.from),parsed.to,parsed.subject.trim,parsed.parsedDate,parsed.attachments,parsed.labels.trim,parsed.body)
    }
  }
    //.filter('success){ success: Int => success ==1 } // only take successfully parsed tuples. Currently not used
    .filter('labels){ labels: String => !(labels.split(",").contains("Chat"))} // ignore Chat messages
    .flatMap('to-> 'to) { to:String=>to.split(",").map{s=>scrubAddresses(s)} } // each recipient gets their own tuple
    .project(finalSchema)

  val personStat = parsedPipe.map('date->'isWeekend){d:DateTime => d.getDayOfWeek > 5}
                  .map('body -> 'mailLength) {b:String => b.length}
                  .map('subject -> 'subjLength) {s:String => s.length}
                  // group by sender and whether it was sent on a weekend. Then count the number of messages in that group
                  // and get the average length of messages in each group along with average subject length in each group
                  .groupBy('from,'isWeekend)(_.size('count).average('mailLength).average('subjLength))
                  .groupAll(_.sortBy('count)) // order by message count

    personStat.write(Csv(args("output"),writeHeader=true))
  /**
   * Function to sanitize email addresses
   * @param addr
   * @return
   */
  def scrubAddresses(addr:String)={
    val toRemove:List[String]=List("\t","\n","\r","\"","'")
    toRemove.foldLeft(addr)((a,rem)=>a.replace(rem,"")).trim
  }

}

/**
 * This case class is the format the JSON is unmarshalled into. Ideally, we would use Option[T] for all the members isntead of
 * String and List[String] because this case class will cause an exception to be thrown if the field is missing. Option[T] prevent that.
 * However, in this case, my JSON is guaranteed to contain all these fields and it makes for a less verbose dereferencing to get members out
 * @param from
 * @param to
 * @param cc
 * @param subject
 * @param date
 * @param labels
 * @param body
 * @param attachments
 */
case class mailJSON(from:String,to:String,cc:String,subject:String,date:String,labels:String,body:String,attachments:List[String])
{
  val dFormat:DateTimeFormatter=DateTimeFormat.forPattern("EEE, MMM dd, yyyy hh:mm aa")
  val parsedDate=DateTime.parse(date,dFormat)
}

