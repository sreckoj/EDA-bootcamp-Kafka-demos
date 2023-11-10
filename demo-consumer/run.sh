#!/bin/bash

. ../env.sh

mvn clean compile
mvn exec:java -Dexec.mainClass=eda.demo.DemoConsumer