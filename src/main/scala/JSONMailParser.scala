import com.twitter.scalding._
import scala.util.parsing.json._
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
import org.json4s._
import org.json4s.native.JsonMethods._

object JSONMailParser {
    def main(args: Array[String]) {
        args.foreach { e=>println(" -> "+ e) }
        ToolRunner.run(new Configuration, new Tool, args)
    }
}


class JSONMailParser(args:Args) extends Job(args)
{
  implicit val formats = org.json4s.DefaultFormats // required for extracting object out of JSON

  val fieldSchema = ('success,'from,'to,'subject,'date,'attachments,'labels)
  val finalSchema = ('from,'to,'subject,'date,'attachments,'labels)

  TextLine(args("input"))
    .map('line->fieldSchema)
  {
    line:String => {
      try{
        val parsed=parse(line).extract[mailJSON] // parse JSON and then "unmarshall" into mailJSON case class
        // See coments for mailJSON case class regarding why these fields aren't wrapped in Option[T]
        (1,parsed.from,parsed.to,parsed.subject,parsed.date,parsed.attachments,parsed.labels)
      }
      catch { // in case of any exceptions, we return a tuple of empty strings. The arity of the tuple in try and catch need to match
        case e:Exception => (0,"","","","","","") // 0 for the value of "success" means it failed
      }
    }
  }
    .filter('success){ success: Int => success ==1 } // only take successfully parsed tuples
    .filter('labels){ labels: String => !(labels.split(",").contains("Chat"))} // ignore Chat messages
    .project(fieldSchema)
    .flatMap('to-> 'to)
  {
    to:String=>
    {
      for {
        addr <- to.split(",")
      }
      yield addr.trim()
    }
  }
    //.groupBy('to) {group=> group.size }
    //.project('subject,'to)
    .project(finalSchema)
    .write(Csv(args("output")))



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

