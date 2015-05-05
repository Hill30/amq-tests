# amq-tests

HOW TO RUN

Example:
\out\artifacts\AMQStressTest_jar>java -jar AMQStressTest.jar 1000 100 "127.0.0.1" "/prefix_" "" "" 30


first argument - number of subscribers = 1000
second argument - number of connect/disconnect cycles = 100
third argument - hostname = "127.0.0.1"
forth argument - client and topic prefix = "/prefix_"
fifth argument - user id = ""
sixth argument - user password = ""
seventh argument - keep alive timeout = 30