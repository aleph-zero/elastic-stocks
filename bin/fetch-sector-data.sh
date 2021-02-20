#!/bin/bash

function usage {
      echo "usage: fetch-sector-data.sh -o <file>"
      echo "    -o  output to file"
      echo ""
}

echo "Download manually from https://www.nasdaq.com/market-activity/stocks/screener"
exit 0

file="sectors-`date '+%Y-%m-%d'`.csv"
while getopts ":ho:" opt; do
  case $opt in
    o)
      file=$OPTARG 
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

if [ -z "$file" ];
then
    usage
    exit 1
fi

echo "Fetching Nasdaq Data"
curl --progress-bar --location --output xxx.txt 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
tail -n +2 xxx.txt > nasdaq.txt
awk '{print "\"nasdaq\","$0}' nasdaq.txt > nasdaq-sectors.txt
rm nasdaq.txt xxx.txt

echo "Fetching NYSE Data"
curl --progress-bar --location --output xxx.txt 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
tail -n +2 xxx.txt > nyse.txt
awk '{print "\"nyse\","$0}' nyse.txt > nyse-sectors.txt
rm xxx.txt nyse.txt

echo "Fetching AMEX Data"
curl --progress-bar --location --output xxx.txt 'http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'
tail -n +2 xxx.txt > amex.txt
awk '{print "\"amex\","$0}' amex.txt > amex-sectors.txt
rm xxx.txt amex.txt

cat amex-sectors.txt nasdaq-sectors.txt nyse-sectors.txt > $file
rm amex-sectors.txt nasdaq-sectors.txt nyse-sectors.txt
