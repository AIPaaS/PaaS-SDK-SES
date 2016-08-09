# Pull base image  
FROM tomcat:8.5.4
  
#change config parameter
RUN sed -i '/\# OS/i JAVA_OPTS="$JAVA_OPTS -server -Xms512M -Xmx512M -XX:PermSize=64M -XX:MaxPermSize=128M  -XX:+UseConcMarkSweepGC -XX:ParallelGCThreads=8 -XX:+PrintCommandLineFlags -XX:+PrintGCDetails -XX:+UseCompressedOops -XX:-UseLargePagesIndividualAllocation -XX:+HeapDumpOnOutOfMemoryError" \n if [[ "$JAVA_OPTS" != *-Djava.security.egd=* ]]; then \n   JAVA_OPTS="$JAVA_OPTS -Djava.security.egd=file:/dev/./urandom" \n  fi'  /usr/local/tomcat/bin/catalina.sh
RUN sed -i '/\/Context/i <Resources cachingAllowed="true" cacheMaxSize="100000" />' /usr/local/tomcat/conf/context.xml
RUN sed -i '/\/Context/i <JarScanner scanClassPath="false" />' /usr/local/tomcat/conf/context.xml
RUN rm -fr /usr/local/tomcat/webapps/*

COPY ./build/libs/iPaaS-Search.war /usr/local/tomcat/webapps/iPaaS-Search.war


# Define default command.  
CMD ["catalina.sh", "run"]