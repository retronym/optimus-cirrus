#!/bin/bash
# Compile and run the tracker-side Tier 1 cache probes (T4 T5 T6 T7 T8 T9 T10 T11 T12 T14 T17 T18).
#   probes/cache/run-tier1-tracker.sh [ids...]      (ids: T4 T4a ... T18b-ideal1; default all)
# Builds only Tier1Tracker.scala + ttracks/Tier1TrackerTT.java into classes-tier1-tracker, against probes/cache/classes
# (the shared fixtures, built by build.sh). Each probe variant runs in its own child JVM (CACHE_JAVA_CMD).
# RESULT lines are appended to RESULTS-tier1-tracker.txt; full child output in logs-tier1-tracker/<id>.log.
# Env: SKIP_BUILD=1, JVM_EXTRA, CIRRUS_BUILD.
P=$(cd $(dirname $0) && pwd)
R=$(cd $P/.. && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
FIX=$P/classes
OUT=$P/classes-tier1-tracker
[ -f $FIX/cacheagent.jar ] || { echo "run $P/build.sh first" >&2; exit 1; }
if [ "${SKIP_BUILD:-0}" != 1 ]; then
  T=$OUT.tmp.$$; rm -rf $T; mkdir -p $T
  SRC=($P/src/optimus/graph/tracking/Tier1Tracker.scala $P/src/optimus/graph/tracking/ttracks/Tier1TrackerTT.java)
  java -Xss8m -cp "$(cat $B/cp.txt)" scala.tools.nsc.Main -nowarn -deprecation:false \
    -Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq \
    -cp "$FIX:$CP" -d $T "${SRC[@]}" || { rm -rf $T; exit 1; }
  javac -nowarn -proc:none -cp "$T:$FIX:$CP" -d $T $P/src/optimus/graph/tracking/ttracks/Tier1TrackerTT.java || { rm -rf $T; exit 1; }
  rm -rf $OUT; mv $T $OUT
fi
AGENTS="-javaagent:$B/jars/entityagent.jar -javaagent:$FIX/cacheagent.jar"
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export CACHE_JAVA_CMD="java -XX:+UseSerialGC $AGENTS $JVM_OPENS -Doptimus.graph.async.profiler=false ${JVM_EXTRA:-} -cp $OUT:$FIX:$CP"
export T1T_RESULTS=$P/RESULTS-tier1-tracker.txt
export T1T_LOGS=$P/logs-tier1-tracker
exec java -XX:+UseSerialGC $AGENTS $JVM_OPENS -Doptimus.graph.async.profiler=false -cp "$OUT:$FIX:$CP" optimus.graph.tracking.Tier1Tracker "$@"
