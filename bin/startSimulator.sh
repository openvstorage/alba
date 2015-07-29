#! /usr/bin/env bash
main=com.seagate.kinetic.simulator.internal.SimulatorRunner
java -classpath ./kinetic-all-0.8.0.4-SNAPSHOT-jar-with-dependencies.jar \
 $main "$@"
