#!/usr/bin/env bash

cd $(dirname $0)
cd ../../../

JAR_FILE=$(ls target |grep jar)
echo $JAR_FILE

PORT=4545

DEFAULT_JMX_OPTS="-Djava.rmi.server.hostname=0.0.0.0
-Dcom.sun.management.jmxremote.port=5678
-Dcom.sun.management.jmxremote.rmi.port=5678
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.ssl=false"

DEFAULT_OOM_OPTS="-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=dumps/seed<pid>_\`date\`.hprof
-XX:+UseGCOverheadLimit"

export INSTANCE_ID=seed

java \
-cp target/${JAR_FILE}:target/lib/* \
-Dlog4j.configurationFile="log4j2-debug.xml" \
-Dlog4j2.contextSelector="org.apache.logging.log4j.core.async.AsyncLoggerContextSelector" \
${JVM_OPTS} ${DEFAULT_JMX_OPTS} ${DEFAULT_OOM_OPTS} \
io.scalecube.issues.i187.SeedRunner $PORT
