#!/bin/bash
# H4 retention soak, optimus.graph.CacheSoak (CACHE_CAMPAIGN.md Tier 2 H4).
#   run-cache-soak.sh a|b <minutes> <seedBase> [sampleSec]     run segments until <minutes> elapse; a hang (exit 3) starts a new segment
#   run-cache-soak.sh --print-cmd
# Env: SOAK_OPTS (JVM flags: -Doptimus.gthread.ideal=4 -Dprobes.delay.maxNanos=50000 [-Doptimus.tracking.cleanup.ttrackClearedNodeRefWatermark=1]
#      -Dhunter.clients=4 -Dhunter.soakDags=20 -Dhunter.soakRoots=3 -Dhunter.soakClients=2 -Dhunter.wdMs=60000;
#      b only: -Dhunter.off=lp (no low-priority evaluations), -Dhunter.soakRoot=N (run only root N),
#      -Dhunter.soakNoRebase=true, -Dhunter.soakNoDispose=true (harness bisection knobs)), SOAK_LOGDIR, SOAK_TAG, SOAK_CLASSES.
# Compiles CacheSoak.scala against probes/cache/classes-hunter (CacheHunter, run run-cache-hunter.sh once first) and probes/cache/classes into
# probes/cache/classes-soak. Runs with the probes/cache/run.sh agents/opens plus the hunter's WorkQueue DelayAgent.
P=$(cd $(dirname $0)/.. && pwd)
C=$P/cache
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
FIX=$C/classes
HUN=$C/classes-hunter
OUT=${SOAK_CLASSES:-$C/classes-soak}
SRC=$C/src/optimus/graph/CacheSoak.scala
[ -f $HUN/delayagent.jar ] || { echo "run $C/run-cache-hunter.sh once first" >&2; exit 1; }
if [ ! -f $OUT/optimus/graph/CacheSoak.class ] || [ -n "$(find $SRC $HUN/delayagent.jar -newer $OUT/optimus/graph/CacheSoak.class)" ]; then
  T=$OUT.tmp.$$; rm -rf $T; mkdir -p $T
  java -Xss8m -cp "$(cat $B/cp.txt)" scala.tools.nsc.Main -nowarn -deprecation:false \
    -Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq \
    -cp "$HUN:$FIX:$CP" -d $T $SRC || { rm -rf $T; exit 1; }
  rm -rf $OUT.old; [ -d $OUT ] && mv $OUT $OUT.old; mv $T $OUT; rm -rf $OUT.old
fi
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
CMD="java -Xss64m -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar -javaagent:$FIX/cacheagent.jar -javaagent:$HUN/delayagent.jar $JVM_OPENS -Doptimus.graph.async.profiler=false ${SOAK_OPTS:-} -cp $OUT:$HUN:$FIX:$CP optimus.graph.CacheSoak"
[ "$1" = "--print-cmd" ] && { echo "$CMD"; exit 0; }
MODE=$1; MIN=$2; SEED=$3; SAMPLE=${4:-300}
LOGDIR=${SOAK_LOGDIR:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-vigilant-ellis-62df0c/6af98b8f-d52a-4e46-b6f5-ebcab3bf3088/scratchpad/cache-soak}
mkdir -p $LOGDIR
END=$(( $(date +%s) + MIN * 60 ))
SEG=0
while :; do
  REM=$(( END - $(date +%s) ))
  [ $REM -lt 60 ] && break
  LOG=$LOGDIR/soak-$MODE${SOAK_TAG:+-$SOAK_TAG}-seg$SEG.log
  echo "SEGMENT $SEG mode=$MODE seedBase=$SEED remSec=$REM log=$LOG cmd: $CMD $MODE $REM $SEED $SAMPLE"
  $CMD $MODE $REM $SEED $SAMPLE > $LOG 2>&1
  RC=$?
  echo "SEGMENT $SEG exit=$RC $(grep -m1 '^HANG-CLASS\|^SOAK-CRASH\|^ROOT-CRASH\|^CHILD-HANG' $LOG)"
  [ $RC -eq 0 ] && break
  [ $RC -ne 3 ] && break
  SEG=$((SEG + 1))
done
