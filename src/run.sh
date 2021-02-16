#!/bin/bash

# Start manager
java -jar ./classes/artifacts/Server/Server.jar localhost:1231 localhost:1231 localhost:1232 false true &
sleep 2
# Start forwarder
java -jar ./classes/artifacts/Server/Server.jar localhost:1232 localhost:1231 localhost:1232 true false &
sleep 2

# Start Servers
java -jar ./classes/artifacts/Server/Server.jar localhost:1233 localhost:1231 localhost:1232 false false &
sleep 2
java -jar ./classes/artifacts/Server/Server.jar localhost:1234 localhost:1231 localhost:1232 false false &
sleep 2

# Start Client
java -jar ./classes/artifacts/Client/Client.jar localhost:1235 localhost:1232
