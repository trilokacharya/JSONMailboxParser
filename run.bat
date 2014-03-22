sbt "run-main JSONMailbox.JobStart JSONMailbox.EmailStats --local -input q:\\tempOut_2.json -output resources/testout -errors resources/errors -output2 resources/testout2"
REM sbt "run-main JSONMailbox.JobStart JSONMailbox.EmailSearch --local  --input q:\\tempOut_2.json --output resources/testout --errors resources/errors --output2 resources/testout2 --search \"test +another\""
