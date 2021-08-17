#!/bin/bash
a=`hostnamectl|grep "hostname"|rev| cut -c'-' -f1-1 |rev`
if [ "$a" == "m" ]
then
gsutil cp gs://glanceaztogcspoc/analytics/dataproc-cluster-initialisation-script/config-m.properties /etc/presto/conf/config.properties
gsutil cp gs://glanceaztogcspoc/analytics/dataproc-cluster-initialisation-script/jvm.config /etc/presto/conf/jvm.config
else
gsutil cp gs://glanceaztogcspoc/analytics/dataproc-cluster-initialisation-script/config.properties /etc/presto/conf/config.properties
gsutil cp gs://glanceaztogcspoc/analytics/dataproc-cluster-initialisation-script/jvm.config /etc/presto/conf/jvm.config
fi
# gsutil cp gs://glanceaztogcspoc/analytics/dataproc-cluster-initialisation-script/config.properties /etc/presto/conf/config.properties
# gsutil cp gs://glanceaztogcspoc/analytics/dataproc-cluster-initialisation-script/jvm.config /etc/presto/conf/jvm.config