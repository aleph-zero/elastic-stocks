#!/bin/bash

if [[ "$PWD" =~ bin ]];
then
    cd ../;
fi

function usage {
      echo "usage: calculate-macd.sh -s -e -i -z"
      echo "    -e  elasticsearch host"
      echo "    -i  elasticsearch index"
      echo "    -s  hdfs path to stocks file"
      echo "    -z  hdfs path to sectors file"
}

host=""
index=""
stocks_input_file=""
sectors_input_file=""

while getopts ":i:s:e:z:" opt; do
    case $opt in
        e)
            host=$OPTARG
            ;;
        i)
            index=$OPTARG
            ;;
        s)
            stocks_input_file=$OPTARG
            ;;
        z)
            sectors_input_file=$OPTARG
            ;;
        h)
          usage
          exit 1
          ;;
        \?)
          usage
          exit 1
          ;;
    esac
done

if [ -z "$host" ];
then
    usage
    exit 1
fi

if [ -z "$index" ];
then
    usage
    exit 1
fi

if [ -z "$stocks_input_file" ];
then
    usage
    exit 1
fi

if [ -z "$sectors_input_file" ];
then
    usage
    exit 1
fi

$HADOOP_HOME/bin/hadoop jar \
    ./target/elastic-stocks-1.0-SNAPSHOT.jar \
    co.elastic.demo.stocks.mapreduce.EndOfDayEquitiesLoader \
    -ef $stocks_input_file \
    -sf $sectors_input_file \
    -ei $index \
    -eh $host

exit 0
