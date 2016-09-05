#! /usr/bin/env bash
main=com.seagate.kinetic.simulator.internal.SimulatorRunner
#jar=./kinetic-all-0.8.0.4-SNAPSHOT-jar-with-dependencies.jar
jar=./kinetic-lamarr.jar
java -classpath $jar $main "$@"
