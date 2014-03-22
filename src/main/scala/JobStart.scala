package JSONMailbox

import com.twitter.scalding.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.conf.Configuration

/**
 * Created by tacharya on 3/21/14.
 */

object JobStart {
  def main(args: Array[String]) {
    ToolRunner.run(new Configuration, new Tool, args)
  }
}
