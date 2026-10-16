#!/bin/bash
# Compile and run Tier 0 probes. Usage: run.sh [probe ids...]; extra JVM args via JVM_EXTRA="..."
P=$(cd $(dirname $0) && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
rm -rf $P/classes; mkdir -p $P/classes
javac -nowarn -proc:none -cp "$CP" -d $P/classes $(find $P/src -name '*.java') || exit 1
# P3 delay-injection agent (probes.DelayAgent; ASM comes from the app class path). No-op unless a
# probe arms probes.Delay. Disable with DELAY_AGENT=0.
DELAY_JAVAAGENT=
if [ "${DELAY_AGENT:-1}" != 0 ]; then
  printf 'Premain-Class: probes.DelayAgent\n' > $P/classes/delayagent.mf
  (cd $P/classes && jar cfm delayagent.jar delayagent.mf probes/DelayAgent*.class) || exit 1
  DELAY_JAVAAGENT="-javaagent:$P/classes/delayagent.jar"
fi
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
CMD="java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar $DELAY_JAVAAGENT $JVM_OPENS -Doptimus.graph.async.profiler=false -cp $P/classes:$CP optimus.graph.Probes"
export PROBES_JAVA_CMD="$CMD"
exec java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar $DELAY_JAVAAGENT $JVM_OPENS -Doptimus.graph.async.profiler=false ${JVM_EXTRA:-} -cp "$P/classes:$CP" optimus.graph.Probes "$@"
