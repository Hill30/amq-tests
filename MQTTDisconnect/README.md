####Test case description:
 * connects to a topic with a requested QoS
 * waits for the connection to be established
 * subscribes to recieve messages from the topic (if requested)
 * disconnects from the topic

The test is repeated the requested number of times.

####Test parameters
 * broker url
 * topic name
 * client name
 * Quality of service
 * repeat counter

####Running instructions
 * Load the source code in the IDE (JetBrains Idea Community edition worked for me)
 * Set the parameters listed above to the desired values. I ran the test for 10000 repetitions

