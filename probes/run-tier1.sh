#!/bin/bash
# Compile and run Tier 1 probes (optimus.graph.Tier1). Usage: run-tier1.sh [T1 T2 T7 T8]; extra JVM args via JVM_EXTRA="...".
# Each scenario runs in child JVMs (PROBES_JAVA_CMD, -D flags inserted after "java"); child logs go to $TIER1_LOGDIR if set.
# Tier1.java is self-contained, so only it is compiled (into classes-tier1, separate from run.sh's classes dir).
P=$(cd $(dirname $0) && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
rm -rf $P/classes-tier1; mkdir -p $P/classes-tier1
javac -nowarn -proc:none -cp "$CP" -d $P/classes-tier1 $P/src/optimus/graph/Tier1.java || exit 1
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export PROBES_JAVA_CMD="java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar $JVM_OPENS -Doptimus.graph.async.profiler=false ${JVM_EXTRA:-} -cp $P/classes-tier1:$CP optimus.graph.Tier1"
[ "$1" = "--print-cmd" ] && { echo "$PROBES_JAVA_CMD"; exit 0; }
exec java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar $JVM_OPENS -Doptimus.graph.async.profiler=false ${JVM_EXTRA:-} -cp "$P/classes-tier1:$CP" optimus.graph.Tier1 "$@"
