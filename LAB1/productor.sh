#!/bin/sh
Acarretera=(A-1 A-2 A-3 A-4 A-5 A-6)
Akmret=(0 0 0 0 0 0 0 0 1 2)
i=0
j=0
while true
do
  imod=`expr $i % 6`
  jmod=`expr $j % 100`
  for pk in {1..40}
  do
    k=$(( RANDOM % 10 ))
    carretera=${Acarretera[$imod]}
    kmret=${Akmret[$k]}
    ts=$(date +"%Y-%m-%d-%T")
    echo "$ts,$carretera,$pk,$kmret"
    aws kinesis put-record --stream-name stream1 --data "$ts,$carretera,$pk,$kmret"$'\n' --partition-key carreteritas --region us-east-1
  done
  i=`expr $i + 1`
  k=`expr $k + 1`
done
