#!/bin/bash
# H2 (tweak-and-reuse) and H3 (invalidation) cache fuzzers, optimus.graph.CacheHunter (CACHE_CAMPAIGN.md Tier 2).
#   run-cache-hunter.sh sweep h2|h3 <firstSeed> <count>        matrix, reruns failures 5x, minimises + classifies per family
#   run-cache-hunter.sh one h2|h3 <seed> <cfg> [reps] [k=v..]   cfg h2: i{ideal}-a{asserts}-d{delay}; h3 adds -w{watermark=1}
#   run-cache-hunter.sh spec h2|h3 <seed> [k=v..]               print the generated spec (after knobs)
#   run-cache-hunter.sh min h2|h3 <seed> <cfg> <family>         minimise + classify one failure family (e.g. ORACLE:UNEXPLAINED)
#   run-cache-hunter.sh selftest [n]                            memoised oracle vs ProbeDag.SequentialEvaluator
# knobs: off=<toggle,..> (tags modes byname when si xs xsft alias pin nest unordered twice cancel) maxProps=N drop=i,j clients=N
# Env: HUNTER_PAR (parallel children, default 8), HUNTER_LOGDIR, HUNTER_MAXMIN (families minimised per sweep, default 4),
#      HUNTER_OPTS (-D flags for the parent, passed on to children: -Dhunter.budgetMs=3000 -Dhunter.iters=200 -Dhunter.delayNanos=50000).
# Compiles only CacheHunter.scala (against probes/cache/classes, the fixtures) and the WorkQueue DelayAgent into
# probes/cache/classes-hunter. Children run with the entity agent, probes.DelayAgent (WorkQueue points) and
# probes.CacheDelayAgent (cache/TTrack/XSFT/queue points); -Dprobes.delay.maxNanos drives random parks at all of them.
P=$(cd $(dirname $0)/.. && pwd)
C=$P/cache
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
FIX=$C/classes
OUT=${HUNTER_CLASSES:-$C/classes-hunter}
SRCS="$C/src/optimus/graph/CacheHunter.scala $P/src/probes/DelayAgent.java"
[ -f $FIX/cacheagent.jar ] || { echo "run $C/build.sh first" >&2; exit 1; }
if [ ! -f $OUT/delayagent.jar ] || [ -n "$(find $SRCS $FIX/cacheagent.jar -newer $OUT/delayagent.jar)" ]; then
  T=$OUT.tmp.$$; rm -rf $T; mkdir -p $T
  java -Xss8m -cp "$(cat $B/cp.txt)" scala.tools.nsc.Main -nowarn -deprecation:false \
    -Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq \
    -cp "$FIX:$CP" -d $T $C/src/optimus/graph/CacheHunter.scala || { rm -rf $T; exit 1; }
  javac -nowarn -proc:none -cp "$FIX:$CP" -d $T $P/src/probes/DelayAgent.java || { rm -rf $T; exit 1; }
  printf 'Premain-Class: probes.DelayAgent\n' > $T/delayagent.mf
  (cd $T && jar cfm delayagent.jar delayagent.mf probes/DelayAgent*.class && rm -rf probes) || { rm -rf $T; exit 1; }
  rm -rf $OUT.old; [ -d $OUT ] && mv $OUT $OUT.old; mv $T $OUT; rm -rf $OUT.old
fi
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export HUNTER_JAVA_CMD="java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar -javaagent:$FIX/cacheagent.jar -javaagent:$OUT/delayagent.jar $JVM_OPENS -Doptimus.graph.async.profiler=false -cp $OUT:$FIX:$CP"
export HUNTER_LOGDIR=${HUNTER_LOGDIR:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-vigilant-ellis-62df0c/6af98b8f-d52a-4e46-b6f5-ebcab3bf3088/scratchpad/cache-hunter}
[ "$1" = "--print-cmd" ] && { echo "$HUNTER_JAVA_CMD"; exit 0; }
exec java -Xss64m ${JVM_EXTRA:-} ${HUNTER_OPTS:-} -cp "$OUT:$FIX:$CP" optimus.graph.CacheHunter "$@"
