#!/bin/bash
# Compile and run the cache/XS-side Tier 1 probes (T1 T2 T3 T13 T15 T16 T19), CACHE_CAMPAIGN.md.
#   probes/cache/run-tier1-cache.sh [probe ids...]        (default: all; each probe runs in child JVMs)
#   NOBUILD=1 skips the compile. JVM_EXTRA is passed to the parent (children get their own flags).
# Compiles probes/cache/src/optimus/graph/cache/Tier1Cache*.scala against probes/cache/classes (fixtures) into
# probes/cache/classes-tier1-cache, so other agents' sources under probes/cache/src are not needed.
P=$(cd $(dirname $0) && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
FIX=$P/classes
OUT=$P/classes-tier1-cache
[ -f $FIX/cacheagent.jar ] || { echo "run $P/build.sh first" >&2; exit 1; }
if [ -z "$NOBUILD" ]; then
  rm -rf $OUT.tmp; mkdir -p $OUT.tmp
  java -Xss8m -cp "$(cat $B/cp.txt)" scala.tools.nsc.Main -nowarn -deprecation:false \
    -Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq \
    -cp "$FIX:$CP" -d $OUT.tmp $P/src/optimus/graph/cache/Tier1Cache*.scala || { rm -rf $OUT.tmp; exit 1; }
  rm -rf $OUT; mv $OUT.tmp $OUT
fi
AGENTS="-javaagent:$B/jars/entityagent.jar -javaagent:$FIX/cacheagent.jar"
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export CACHE_JAVA_CMD="java -XX:+UseSerialGC $AGENTS $JVM_OPENS -Doptimus.graph.async.profiler=false -cp $OUT:$FIX:$CP"
export TIER1_CACHE_RESULTS=$P/RESULTS-tier1-cache.txt
exec java -XX:+UseSerialGC $JVM_OPENS ${JVM_EXTRA:-} -cp "$OUT:$FIX:$CP" optimus.graph.cache.Tier1Cache "$@"
