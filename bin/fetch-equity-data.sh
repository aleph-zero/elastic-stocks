#!/bin/bash

function usage {
      echo "usage: fetch-equity-data.sh -f -p -a -k <apikey>"
      echo "    -f  download full historical data"
      echo "    -p  download previous day's data"
      echo "    -s  start date, ex: 2019-02-01"
      echo "    -e  end date, ex: 2019-12-31"
      echo "    -o  output to file"
      echo "    -k  quandl api key"
      echo ""
}

start_date=""
end_date=""
apikey=""
range=false
full=false
partial=false
file="equities-`date '+%Y-%m-%d'`.csv"

while getopts ":hk:fpo:s:e:" opt; do
  case $opt in
    k)
      apikey=$OPTARG
      ;;
    f)
      full=true 
      ;;
    p)
      partial=true 
      ;;
    s)
      start_date=$OPTARG
      full=false 
      partial=false 
      range=true
      ;;
    e)
      end_date=$OPTARG
      full=false 
      partial=false 
      range=true
      ;;
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

if [ -z "$apikey" ];
then
    apikey=$QUANDL_API_KEY
fi

if [ -z "$apikey" ];
then
    usage
    exit 1
fi

if [ -z "$file" ];
then
    usage
    exit 1
fi


if $range;
then
    echo "Fetching Historical Equity Data $start_date to $end_date"
    echo "curl --output equities.zip --progress-bar --location 'https://www.quandl.com/api/v3/databases/EOD/data?start_date=$start_date&end_date=$end_date&api_key=$apikey'"
    exit 1
fi

if $full || $partial;
then
    if $full;
    then
        echo "Fetching Historical Equity Data"
        curl --output equities.zip --progress-bar --location "https://www.quandl.com/api/v3/databases/EOD/data?api_key=$apikey"
        unzip -p equities.zip > $file || exit 1
        rm -f equities.zip
    else
        echo "Fetching Previous Day's Equity Data"
        curl --output equities.zip --progress-bar --location "https://www.quandl.com/api/v3/databases/EOD/data?api_key=$apikey&download_type=partial"
        unzip -p equities.zip > $file || exit 1
        rm -f equities.zip
    fi
else
    usage
    exit 1
fi

