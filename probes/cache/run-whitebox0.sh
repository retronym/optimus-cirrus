#!/bin/bash
# Compile and run the fixture-free Tier 0 cache probes (W1 W5 W6 W7 W12 W14).
# Usage: run-whitebox0.sh [probe ids...]   extra JVM args via JVM_EXTRA="..."
# W1 under the default width (6 qwords) and under width 2:
#   run-whitebox0.sh W1; JVM_EXTRA=-Doptimus.graph.tweakUsageQWords=2 run-whitebox0.sh W1
#   JVM_EXTRA=-Doptimus.graph.tweakUsageQWords=0 run-whitebox0.sh W1   (per-node masks off; TPDMask not rewritten)
P=$(cd $(dirname $0) && pwd)
R=$(cd $P/.. && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done)"
OUT=$P/classes-whitebox0
rm -rf $OUT; mkdir -p $OUT
javac -nowarn -proc:none -cp "$CP" -d $OUT \
  $P/src/optimus/graph/cache/Whitebox0.java \
  $P/src/optimus/graph/tracking/ttracks/TTrackProbeSupport.java \
  $R/src/probes/Steps.java || exit 1
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
exec java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar $JVM_OPENS -Doptimus.graph.async.profiler=false ${JVM_EXTRA:-} -cp "$OUT:$CP" optimus.graph.cache.Whitebox0 "$@"
