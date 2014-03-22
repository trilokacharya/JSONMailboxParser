package JSONMailbox

import com.twitter.scalding._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.json4s._
import org.json4s.native._ //JsonMethods._
import org.joda.time.DateTime

trait JSONMailParser extends Job
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
    //.filter('labels){ labels: String => !(labels.split(",").contains("Chat"))} // ignore Chat messages
     .filter('labels){ labels: String => (labels.split(",").contains("Chat"))} // only Chat messages
    .flatMap('to-> 'to) { to:String=>to.split(",").map{s=>scrubAddresses(s)} } // each recipient gets their own tuple
    .project(postFilterSchema)

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

