# amq-tests

HOW TO RUN

It uses fusesource mqtt client (https://github.com/fusesource/mqtt-client)

Example:
\out\artifacts\AMQStressTest_jar>java -jar AMQStressTest.jar 1000 100 "127.0.0.1" "/prefix_" "" "" 30

first argument - number of subscribers = 1000
second argument - number of connect/disconnect cycles = 100
third argument - hostname = "127.0.0.1"
forth argument - client and topic prefix = "/prefix_"
fifth argument - user id = ""
sixth argument - user password = ""
seventh argument - keep alive timeout = 30

You can also run mqtt_conncect-disconnect\out\artifacts\AMQStressTest_jar\AMQStressTest.jar like java -jar AMQStressTest_jar
Client would try to open 1000 connections wait for a few seconds and close it. Then it will try it again One thousand times. Until it would fail either way: from server or client side.