@echo off

REM ******* PARAMS *************

set APP_NAME="amq-tests-openwire-publisher.jar"
set NODE1_URL="tcp://10.0.1.146:61616"
set NODE2_URL="tcp://10.0.1.103:61616"
set THREADS_PER_PUBLISHER=1
set TOPICS_NUMBER=20000
set SEND_MESSAGES_TO_SUBSCRIBER=1


REM ******* DESC *************
REM Application sends N messages to cluster nodes
REM Overall number of messages is multiply of TOPICS_NUMBER, SEND_MESSAGES_TO_SUBSCRIBER, THREADS_PER_PUBLISHER (ex. 10 topics * 10 messages * 2 threads = 200 total)
REM You can manage ranges with %%N and %TOPICS_NUMBER parameters:
REM offset = %%N * %TOPICS_NUMBER
REM limit = (%%N * %TOPICS_NUMBER) + %TOPICS_NUMBER

REM **********              *********
REM * pub1 * --------------> * NODE2 *
REM **********              *********

REM **********              *********
REM * pub2 * --------------> * NODE1 *
REM **********              *********

REM ******** TEST ************
for %%N in (0) do start "Publisher %%N" java -jar %APP_NAME% %NODE1_URL% %%N %SEND_MESSAGES_TO_SUBSCRIBER% %TOPICS_NUMBER% %THREADS_PER_PUBLISHER%
for %%N in (0) do start "Publisher %%N" java -jar %APP_NAME% %NODE2_URL% %%N %SEND_MESSAGES_TO_SUBSCRIBER% %TOPICS_NUMBER% %THREADS_PER_PUBLISHER%
