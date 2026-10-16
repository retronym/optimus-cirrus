#!/bin/bash
# Hand-written minimal reproducers for D17 (fuzzer N2) and D18 (fuzzer N3): optimus.graph.Repro17_18.
#   probes/cache/run-repro.sh d17 [reps]       by-name TweakNode poisoned by a cancelled requester (deterministic)
#   probes/cache/run-repro.sh d18 [reps] ...   stale value in a late consistent child under a concurrent child
# Compiles src/optimus/graph/Repro17_18.scala against probes/cache/classes (fixtures) + classes-hunter (CacheHunter)
# + the build classpath into probes/cache/classes-repro; runs with the same agents/opens as probes/cache/run.sh.
P=$(cd $(dirname $0)/.. && pwd)
C=$P/cache
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
FIX=$C/classes
HUN=$C/classes-hunter
OUT=$C/classes-repro
SRC=$C/src/optimus/graph/Repro17_18.scala
[ -f $FIX/cacheagent.jar ] || { echo "run $C/build.sh first" >&2; exit 1; }
if [ ! -f $OUT/.stamp ] || [ -n "$(find $SRC -newer $OUT/.stamp)" ]; then
  rm -rf $OUT; mkdir -p $OUT
  java -Xss8m -cp "$(cat $B/cp.txt)" scala.tools.nsc.Main -nowarn -deprecation:false \
    -Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq \
    -cp "$HUN:$FIX:$CP" -d $OUT $SRC || exit 1
  touch $OUT/.stamp
fi
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
AGENTS="-javaagent:$B/jars/entityagent.jar"
[ "${CACHE_AGENT:-1}" != 0 ] && AGENTS="$AGENTS -javaagent:$FIX/cacheagent.jar"
exec java -XX:+UseSerialGC $AGENTS $JVM_OPENS -Doptimus.graph.async.profiler=false ${JVM_EXTRA:-} -cp "$OUT:$HUN:$FIX:$CP" optimus.graph.Repro17_18 "$@"
