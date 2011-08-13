#!/bin/sh

scp -i ~/.ssh/hackreduce.pem build/libs/HackReduce-0.2.jar hadoop@hackreduce-cluster-2.hopper.to:~/users/team-rim

