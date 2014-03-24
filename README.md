JSONMailboxParser
=================

Scalding project to play with JSON files created from Gmail's mbox output.

Calculating statistics on email senders for some toy problem scenarios. Uses filter, map, flatmap, group, foldLeft and join functionality.

Here are some notes regarding the Scalding setup:

 https://trilokacharya.github.io/sbt_scalding_takeout.html 
 
I use a build.sbt file as the bootstrap for fetching all dependencies, including Scalding. I also have a simple script that runs my Scalding jobs off sbt and doesn't require using scald.rb .

