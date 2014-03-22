package JSONMailbox

import com.twitter.scalding._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.json4s._
import org.json4s.native._ //JsonMethods._
import org.joda.time.DateTime

/**
 * Created by tacharya on 3/21/14.
 */
class EmailStats(args:Args) extends Job(args) with JSONMailParser{

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
}
