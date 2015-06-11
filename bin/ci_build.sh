#! /bin/bash
/data/home/jenkins/bin/reverse_patch.sh  
/usr/maven/bin/mvn install site cobertura:cobertura findbugs:findbugs
/bin/mkdir ./xml_test_report -p; /bin/cp $(/usr/bin/find . -name "TEST*xml") ./xml_test_report
