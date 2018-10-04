#!/bin/bash
function timer()
{
    if [[ $# -eq 0 ]]; then
        echo $(date '+%s')
    else
        local  stime=$1
        etime=$(date '+%s')
#        if [[ -z "$stime" ]]; then stime=$etime; fi
        dt=$((etime - stime))
        #ds=$((dt % 60))
        #dm=$(((dt / 60) % 60))
        #dh=$((dt / 3600))
       # printf '%d sec' $dt
        echo $dt
    fi
}

function runcommand {
    if [ "X$DEBUG_SCRIPT" != "X" ]; then
        $1
    else
        $1 2>/dev/null
    fi
}

usage="sh .runtpc.sh [tpch|tpcds] [scale: 2..] [range: 1 .. 99]"

numreg="^[0-9]+$"

type=$1
scale=$2
#range=$3

#if [ "$#" -eq 2 ]; then
#  range=10
#elif [ "$#" -eq 3 ]; then
#  range=$3
#fi

if [ "$type" = "tpcds" ]; then
  DB="${type}_bin_partitioned_orc_${scale}"
elif [ "$type" = "tpch" ]; then
  DB="${type}_flat_orc_${scale}"
else
 echo $usage
 exit -1
fi

###

range=2

#if [ "$#" -eq 2 ]; then
#  echo "default rang is [query1, query10]"
#  range=10
##[ "$#" -eq 3 ] &&
#fi

if [[ "$#" -eq 3 ]] && [[ $3 =~ $numreg ]]; then
   range=$3
   #echo "test: range = $range"
else
    echo "need a number but get: $3, range=$range.  $usage"
    exit -1
fi

DIR="sample-queries-${type}"

##for HDP
HIVE="beeline -u 'jdbc:hive2://localhost:2181/${DB};auth=noSasl;initFile=${DIR}/testbench.settings;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2;' "

## for EMR
#HIVE="beeline -u 'jdbc:hive2://localhost:10000/${DB};auth=noSasl;initFile=${DIR}/testbench.settings;' "
## HIVE="beeline -u jdbc:hive2://${SERVER}:${PORT} -i settings/load-flat.sql --hivevar DB=tpcds_text_${SCALE} --hivevar LOCATION=${DIR}/${SCALE} -f ddl-tpcds/text/alltables.sql"

echo  "query, time, (range:${range})"

i=1
until [ $i -gt $range ]
do
 # echo "test run query${i}.sql"
  tmr=$(timer)
  { 
    result=$(eval "$HIVE -n hive --outputformat=dsv --silent=true -f ${DIR}/query${i}.sql")
   } &> /dev/null
  rt=$?
 # echo "test rt=$rt"

## > ${DIR}/query$i.sql.log  "
  
  # echo "test: runtime = $runtime"
  if [ $rt==0 ]; then
    goodbad="success"
  else
    goodbad="failed"
  fi

  runtime=$(timer $tmr)
  
  printf "query%d, %d, %s\n" $i $runtime $goodbad
  echo "$result" > "${DIR}/query${i}.sql.log"
  ((i++))
  sleep 3
done
