#!/bin/bash --login
SOURCE="${BASH_SOURCE[0]}"
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

export TWS_API_ADDR=127.0.0.1
export TWS_API_PORT=23456
export TWS_API_CLIENTID=1000

source $DIR/../uranus/conf/env2.sh

cd $DIR
[ -z $1 ] && echo "Need args" && exit 1
if [[ $1 == src ]]; then
	shift
	mvn compile && mvn exec:java $@
else
	echo "Unknown args $@"
	exit 1
fi
