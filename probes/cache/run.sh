#!/bin/bash
# Run a main class from probes/cache/classes with the entity agent, the cache delay agent and the usual opens.
#   probes/cache/run.sh <main-class> [args...]
# Env: JVM_EXTRA (extra JVM flags, e.g. -Doptimus.scheduler.cacheSize=16), CACHE_AGENT=0 (no CacheDelayAgent),
#      CIRRUS_BUILD, CACHE_CLASSES. Exports CACHE_JAVA_CMD (the full java command minus the main class) so a probe
#      can start child JVMs the same way (insert -D flags after "java ").
P=$(cd $(dirname $0)/.. && pwd)
C=$P/cache
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
OUT=${CACHE_CLASSES:-$C/classes}
[ -f $OUT/cacheagent.jar ] || { echo "run $C/build.sh first" >&2; exit 1; }
[ $# -ge 1 ] || { echo "usage: $0 <main-class> [args...]" >&2; exit 2; }
AGENTS="-javaagent:$B/jars/entityagent.jar"
[ "${CACHE_AGENT:-1}" != 0 ] && AGENTS="$AGENTS -javaagent:$OUT/cacheagent.jar"
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export CACHE_JAVA_CMD="java -XX:+UseSerialGC $AGENTS $JVM_OPENS -Doptimus.graph.async.profiler=false -cp $OUT:$CP"
exec java -XX:+UseSerialGC $AGENTS $JVM_OPENS -Doptimus.graph.async.profiler=false ${JVM_EXTRA:-} -cp "$OUT:$CP" "$@"
