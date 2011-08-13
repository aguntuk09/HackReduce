#!/bin/sh
java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.amazonreviews.RecordCounter datasets/amazon/reviews /tmp/hack_counts
