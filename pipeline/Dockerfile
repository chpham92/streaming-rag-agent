# bitnami/spark ships a JVM + PySpark already wired together correctly —
# building that from scratch (matching Spark's Scala/Py4J version pins) has
# a much worse effort-to-payoff ratio than accepting the base image's few
# hundred extra MB for a portfolio project. bitnamilegacy, not bitnami:
# Bitnami moved its free-tier images to this org mid-2025 and stopped
# publishing new tags under the old one — bitnami/spark:3.5 now 404s.
FROM bitnamilegacy/spark:3.5.6

USER root
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY chunking.py stream_indexer.py ./

# uid 1001 genuinely has no /etc/passwd entry in this image (confirmed with
# `getent passwd 1001` — nothing). com.sun.security.auth.module.UnixLoginModule
# is a native/JNI module that reads the OS passwd database directly via
# getpwuid(), so neither a HADOOP_USER_NAME env var (a Hadoop-level fallback
# that never gets reached) nor a JAVA_TOOL_OPTIONS -Duser.name system
# property (which this native module doesn't consult at all) can fix it —
# both were tried and both left the exact same "invalid null input: name"
# crash in spark-submit's own launcher JVM. Since the UID is fixed (we set
# it ourselves below, not an arbitrary runtime UID), the actual fix is
# simpler than either: give it a real passwd entry at build time.
RUN echo "spark:x:1001:0:Spark job user:/tmp:/bin/bash" >> /etc/passwd
USER 1001

# --packages triggers an Ivy resolution of the Kafka connector (and its
# transitive deps: kafka-clients, commons-pool2) from Maven Central on
# container start rather than at image build time — acceptable for local
# dev, but a real deployment would vendor these jars into the image so a
# cold start doesn't depend on Maven Central being reachable.
#
# spark.jars.ivy=/tmp/.ivy2, not the default (~/.ivy2): uid 1001 has no
# /etc/passwd entry in this image, so Java's user.home resolves to the
# literal string "?" and Ivy fails trying to create "?/.ivy2/local". This
# conf points Ivy at an explicit, always-writable path instead of relying
# on user.home resolution inside the container.
ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
            "--conf", "spark.jars.ivy=/tmp/.ivy2", \
            "/app/stream_indexer.py"]
