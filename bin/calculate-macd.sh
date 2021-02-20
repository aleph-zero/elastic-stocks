#!/bin/bash

if [[ "$PWD" =~ bin ]];
then
    cd ../;
fi

function usage {
      echo "usage: calculate-macd.sh -s -e -i"
      echo "    -s  start date yyyy-mm-dd"
      echo "    -e  end date yyyy-mm-dd"
      echo "    -i  elasticsearch index"
      echo "    -l  comma-separated list of stock symbols"
      echo "    -5  include all S&P 500 stocks"
}

input_index=""
start_date=""
end_date=""
stock_list=""
sp500=false

while getopts ":i:s:e:l:5" opt; do
    case $opt in
        s)
            start_date=$OPTARG
            ;;
        e)
            end_date=$OPTARG
            ;;
        i)
            input_index=$OPTARG
            ;;
        l)
            stock_list=$OPTARG
            ;;
        5)
            sp500=true
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

if [ -z "$start_date" ];
then
    usage
    exit 1
fi

if [ -z "$end_date" ];
then
    usage
    exit 1
fi

if [ -z "$input_index" ];
then
    usage
    exit 1
fi

if [ -z "$stock_list" ];
then
    usage
    exit 1
fi

if [[ -n "$input_index" && -n "$start_date" && -n "$end_date" ]];
then
    $HADOOP_HOME/bin/hadoop jar \
        ./target/elastic-stocks-1.0-SNAPSHOT.jar \
        co.elastic.demo.stocks.mapreduce.MovingAverageConvergenceDivergenceCalculator \
        -sd $start_date \
        -ed $end_date \
        -si $input_index \
        -di $input_index \
        -eh "localhost:9200" \
        -ss $stock_list
else
    usage
    exit 1
fi

