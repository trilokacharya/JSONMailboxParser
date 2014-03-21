import cascading.operation.Buffer
import cascading.pipe.Pipe
import cascading.tuple.Fields
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
  val postFilterSchema = ('from,'to,'subject,'date,'attachments,'labels,'body)

  val parsedPipe=TextLine(args("input"))
    .addTrap(Tsv(args("errors")))
    .map('line->fieldSchema)
  {
    line:String => {
      try{
        val parsed=JsonMethods.parse(line).extract[mailJSON] // parse JSON and then "unmarshall" into mailJSON case class
        // See comments for mailJSON case class regarding why these fields aren't wrapped in Option[T]
        (1,scrubAddresses(parsed.from),parsed.to,parsed.subject.trim,parsed.parsedDate,parsed.attachments,parsed.labels.trim,parsed.body)
      }catch{
        case _ =>(0,"","","","","","","") // Any parsing error is getting ignored right now. We filter these out in the next step
      }
    }
  }
    .filter('success){ success: Int => success ==1 } // only take successfully parsed tuples.
    .filter('labels){ labels: String => !(labels.split(",").contains("Chat"))} // ignore Chat messages
    .flatMap('to-> 'to) { to:String=>to.split(",").map{s=>scrubAddresses(s)} } // each recipient gets their own tuple
    .project(postFilterSchema)

  val personStat = parsedPipe.map('date->'isWeekend){d:DateTime => d.getDayOfWeek > 5}
    .map('body -> 'mailLength) {b:String => b.length}
    .map('subject -> 'subjLength) {s:String => s.length}
    // group by sender and whether it was sent on a weekend. Then count the number of messages in that group
    // and get the average length of messages in each group along with average subject length in each group
    .groupBy('from,'isWeekend)(_.size('count).average('mailLength).average('subjLength)) // we split up each person's
                                                    // totals into 2. 1 for weekdays and another for weekends
    .groupAll(_.sortBy('count)) // order by message count , in case we're writing it out here

  //personStat.write(Csv(args("output2"),writeHeader=true))
  // Could end processing here if this is all we wanted to get

  // In the next step, we count the number of contiguous days that emails are exchanged. Once the number of contiguous
  // days reaches a streakThreshold, we do increment the number of streaks with that person
  val streakThreshold = 3

  val df = DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:SS")

  val contiguousThread = parsedPipe
    .filter('date) { date:DateTime => date!=null } // skip any null dates since we'll be comparing dates next
    .groupBy('from)
    {
      _.sortBy('date)
       .foldLeft(('date)->('date,'streak,'totals)) (DateTime.parse("01-01-1990 00:01:00",df),0,0)
        {(lastRow:(DateTime, Int, Int),currRow:(DateTime)) =>
          {
            if(currRow.getMillis-lastRow._1.getMillis > 24*60*60*1000) (currRow,0,lastRow._3) // non contiguous dates, so streak resets
            else if(lastRow._2 == streakThreshold) (currRow,0,lastRow._3+1) // today is the streakThreshold day, so we count it as a contiguous series and increase total. Streak is reset
            else (currRow,lastRow._2 + 1,lastRow._3) // today is a contiguous day, but not yet streakThreshold day streak
          }
        }
    }
    .map('date->'date) {date:DateTime => date.toString(df)}
    .filter('totals) { tot:Int => tot > 1}
    .project('from,'totals)
    .joinWithSmaller('from->'from,personStat) //demonstrating a join. personStat is split into weekday/weekend stats, so
                                              // we get 2 lines per person
    .groupAll(_.sortBy('from))

  contiguousThread
    .write(Csv(args("output"),writeHeader=true))


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
 * This case class is the format the JSON is unmarshalled into. Ideally, we would use Option[T] for all the members instead of
 * String and List[String] because this case class will cause an exception to be thrown if the field is missing. Option[T] prevents that.
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
  //val dFormat:DateTimeFormatter=DateTimeFormat.forPattern("EEE, MMM dd, yyyy hh:mm aa")
  val dFormat:DateTimeFormatter=DateTimeFormat.forPattern("dd-MM-yyyy HH:mm:SS")
  val parsedDate=try {
    DateTime.parse(date,dFormat)}
  catch{
    case _=> DateTime.parse("01-01-1900 00:01:00",dFormat) // default date
  }
}

