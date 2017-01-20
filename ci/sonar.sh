#!/bin/bash -ex

pushd `dirname $0`/.. > /dev/null
root=$(pwd -P)
popd > /dev/null

rm -rf $HOME/.m2/*

mvn clean org.jacoco:jacoco-maven-plugin:prepare-agent install -Pcoverage-per-test org.jacoco:jacoco-maven-plugin:report -DdataFile=$root/target/jacoco.exec
