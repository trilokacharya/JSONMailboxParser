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

  val fieldSchema = ('success,'from,'to,'subject,'date,'attachments,'labels,'body,'comment)
  val finalSchema = ('from,'to,'subject,'date,'attachments,'labels,'comment)

  TextLine(args("input"))
    .map('line->fieldSchema)
  {
    line:String => {
      try{
        val parsed=JsonMethods.parse(line).extract[mailJSON] // parse JSON and then "unmarshall" into mailJSON case class
        // See coments for mailJSON case class regarding why these fields aren't wrapped in Option[T]
        (1,scrubAddresses(parsed.from),parsed.to,parsed.subject.trim,parsed.parsedDate.toString("yyyy:MM:dd"),parsed.attachments,parsed.labels.trim,parsed.body)
      }
      catch { // in case of any exceptions, we return a tuple of empty strings. The arity of the tuple in try and catch need to match
        case e:Exception => (0,"","","","","","","") // 0 for the value of "success" means it failed
      }
    }
  }
    //.filter('success){ success: Int => success ==1 } // only take successfully parsed tuples
    .filter('labels){ labels: String => !(labels.split(",").contains("Chat"))} // ignore Chat messages
    .project(fieldSchema)
    .flatMap('to-> 'to) { to:String=>to.split(",").map{s=>scrubAddresses(s)} } // each recipient gets their own tuple
/*    .groupBy('to,'date) {
          grp => grp.size
                    //.average(grp.)

     }*/
    //.project('subject,'to)
    .project(finalSchema)
    .write(Csv(args("output")))

  /**
   * Function to sanitize email addresses
   * @param addr
   * @return
   */
  def scrubAddresses(addr:String)={
    val toRemove:List[String]=List("\t","\n","\r","\"","'")
    toRemove.foldLeft(addr)((a,rem)=>a.replace(rem,"")).trim
    //addr.replace("\"","").replace("'","").trim
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

