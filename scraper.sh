#!/bin/bash
location=$1
if [[ -z ${location} ]]; then
 echo "Need file containing query"
 exit
fi

logdir="logs/"
if [[ ! -d ${logdir} ]]; then
 mkdir ${logdir}
fi

# scrapy crawl TweetScraper -a query=thor near "manhatan"
genrelist=`ls Genre/`
for fname in ${genrelist}
do
  cat Genre"/"${fname} | while read line
  do
   c=`echo $line | awk '{split($0,a,";" ); print a[1]}'`
   t=`echo $line | awk '{split($0,a,";" ); print a[2]}'`
   query=""${c}" near:\""${location}"\" since:"${t}"-01-01"
   filename=${query//:/-}
   #echo ${c}
   #echo ${t}
   #echo ${query}
   #echo ${filename}
   echo -e "Start crawl ${query}... with shell\n\t (scrapy crawl TweetScraper -a query="${query}" -a lang=en -a filename="${filename//" "/_}" -a location="${location}" -a category="${fname}"> "${logdir}${filename}.log" 2>&1)"
   scrapy crawl TweetScraper -a query="${query}" -a lang=en -a filename="${filename//" "/_}" -a location="${location}" -a category="${fname}" > "${logdir}${filename}.log" 2>&1
   wait
  done
 wait
done
