@echo off

REM *******PARAMS*************
set APP_NAME="amq-tests-mqtt-subscriber.jar"
set NODE1_URL="tcp://10.0.1.146:61616"
set NODE2_URL="tcp://10.0.1.103:61616"
set TOPICS_NUMBER=2000

REM ********* DESC ***********
REM Application creates cross-connections between cluster nodes
REM Each subscription has own Topic to listen(ex. Topic.j0 )
REM You can manage ranges with %%N and %TOPICS_NUMBER parameters:
REM offset = %%N * %TOPICS_NUMBER
REM limit = (%%N * %TOPICS_NUMBER) + %TOPICS_NUMBER

REM **********    0-4       *********
REM * sub * --------------> * NODE1 *
REM **********              *********

REM **********   5-9        *********
REM * sub * --------------> * NODE2 *
REM **********              *********

REM ********************

REM ******* TEST ******
REM 20.000 connections/subscriptions per cluster or 10.000 per node
for %%N in (0,1,2,3,4) do start "Subscriber %%N" java -jar %APP_NAME% %NODE1_URL% %TOPICS_NUMBER% %%N
for %%N in (5,6,7,8,9) do start "Subscriber %%N" java -jar %APP_NAME% %NODE2_URL% %TOPICS_NUMBER% %%N
