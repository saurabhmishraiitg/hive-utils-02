#!/bin/bash

###Parameters###
#Parameter 1 : artifact file name
#Parameter 2 : HTTP Server URL
#Parameter 3 : HTTP Server Port
#Parameter 4 : HTTP Server Username
#Parameter 5 : HTTP Server Password

echo "Current Working Directory `pwd`"
echo "parameters ${@}"
curl -i --user ${4}:${5} -X POST -F upfile=@${1}.jar "http://${2}:${3}/"
#curl -i --user nexus:nexus -X POST -F upfile=sample.dat "http://xxx.org:2121"
