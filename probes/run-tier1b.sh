#!/bin/bash
# Compile and run the remaining Tier 1 probes (optimus.graph.Tier1b). Usage: run-tier1b.sh [T3 T4 T5 T6 T9 T10 T11 T12]; extra parent JVM args via JVM_EXTRA="...".
# Each scenario runs in child JVMs (PROBES_JAVA_CMD, -D/-X flags inserted after "java"); child logs go to $TIER1B_LOGDIR.
# Compiles only Tier1b.java into classes-tier1b. Children carry a third agent, optimus.graph.Tier1bAgent
# (rewrites optimus.core.IndexedArrayList.foreach for T3; hooks are inert for every other list).
P=$(cd $(dirname $0) && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
C=$P/classes-tier1b
rm -rf $C; mkdir -p $C
javac -nowarn -proc:none -cp "$CP" -d $C $P/src/optimus/graph/Tier1b.java || exit 1
printf 'Premain-Class: optimus.graph.Tier1bAgent\n' > $C/tier1bagent.mf
(cd $C && jar cfm tier1bagent.jar tier1bagent.mf optimus/graph/Tier1bAgent*.class) || exit 1
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export PROBES_JAVA_CMD="java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar -javaagent:$C/tier1bagent.jar $JVM_OPENS -Doptimus.graph.async.profiler=false -cp $C:$CP optimus.graph.Tier1b"
export TIER1B_LOGDIR=${TIER1B_LOGDIR:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/tier1b}
[ "$1" = "--print-cmd" ] && { echo "$PROBES_JAVA_CMD"; exit 0; }
exec java ${JVM_EXTRA:-} -cp "$C:$CP" optimus.graph.Tier1b "$@"
