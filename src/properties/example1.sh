#!/usr/bin/env bash
#java -cp libs/*.jar:. serverchainsimulator.threads.Sample main.properties adresses.properties $*
java -cp .:libs/jsch-0.1.53.jar:libs/logback-classic-1.1.3.jar:libs/logback-core-1.1.3.jar:libs/server-chain-simulator-1.0-SNAPSHOT.jar:libs/slf4j-api-1.7.13.jar serverchainsimulator.ServerChainSimulator main.properties adresses.properties $*