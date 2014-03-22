package JSONMailbox

import com.twitter.scalding._

/**
 * Created by tacharya on 3/21/14.
 */
class EmailSearch(args:Args) extends Job(args) with JSONMailParser {

  val searchPattern=args("search")

  println(searchPattern)

  val searchPipe=parsedPipe.filter('body){b:String=>val lower=b.toLowerCase
                                lower.matches(searchPattern)}
                          .filter('to){to:String=> !to.toLowerCase.contains("")}
                          .filter('from){from:String=> !from.toLowerCase.contains("")}

  searchPipe.write(Csv(args("output")))

}
