import com.twitter.scalding._
import scala.util.parsing.json._
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration
//import org.json4s._
//import org.json4s.native.JsonMethods._

object JSONMailParser {
    def main(args: Array[String]) {
        args.foreach { e=>println(" -> "+ e) }
        ToolRunner.run(new Configuration, new Tool, args)
    }
}


class JSONMailParser(args:Args) extends Job(args)
{
    val fieldSchema = ('success,'from,'to,'subject,'date,'attachments,'labels);
    val finalSchema = ('from,'to,'subject,'date,'attachments,'labels);

    TextLine(args("input"))
        .map(('line)->fieldSchema)
        {
            line:String => {
                JSON.parseFull(line) match {
                    case Some(data:Map[String,String]) => 
                        (1,data("from"),data("to"),data("subject"),data("date"),data("attachments"),data("labels"))
                    case None => (0,"","","","","","")
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
