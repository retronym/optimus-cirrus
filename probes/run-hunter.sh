#!/bin/bash
# H3/H5 hunter (optimus.graph.Hunter). Usage:
#   run-hunter.sh sweep <firstSeed> <count>          full matrix, reruns failures 5x, minimises N
#   run-hunter.sh one <seed> <cfg> [maxN] [reps]     cfg = i{ideal}-a{asserts 0|1}-d{delay 0|1}-lt{limitThreads 0|1}
#   run-hunter.sh spec <seed>...                      print the generated DAG summary
#   run-hunter.sh inv [n2:]<cfg>...                   deterministic scenarios: N3 stale-causality inversion; n2: prefix = N2 poisoned evaluateNodeAsync starvation
# Env: HUNTER_LOGDIR (per-child logs), HUNTER_PAR (parallel children, default 4), JVM_EXTRA (parent JVM),
#      HUNTER_OPTS (-D flags for the parent, e.g. -Dhunter.budgetMs=2500 -Dhunter.delayNanos=50000).
# Compiles only Hunter.java and probes/src/probes/{Delay,DelayAgent,Steps}.java into classes-hunter (HUNTER_CLASSES overrides; never touches classes/).
# Children run with two extra agents: probes.DelayAgent (WorkQueue points) and optimus.graph.HunterAgent (scheduler/waiter points + oracle hook).
P=$(cd $(dirname $0) && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
C=${HUNTER_CLASSES:-$P/classes-hunter}
SRCS="$P/src/optimus/graph/Hunter.java $P/src/probes/Delay.java $P/src/probes/DelayAgent.java $P/src/probes/Steps.java"
# Rebuild only when a source is newer than the last build, and swap the directory in one rename, so that a
# second invocation never deletes the jars under child JVMs that a running sweep is still starting.
if [ ! -f $C/hunteragent.jar ] || [ -n "$(find $SRCS -newer $C/hunteragent.jar)" ]; then
  T=$C.tmp.$$; rm -rf $T; mkdir -p $T
  javac -nowarn -proc:none -cp "$CP" -d $T $SRCS || exit 1
  printf 'Premain-Class: probes.DelayAgent\n' > $T/delayagent.mf
  (cd $T && jar cfm delayagent.jar delayagent.mf probes/DelayAgent*.class) || exit 1
  printf 'Premain-Class: optimus.graph.HunterAgent\n' > $T/hunteragent.mf
  (cd $T && jar cfm hunteragent.jar hunteragent.mf optimus/graph/HunterAgent*.class) || exit 1
  rm -rf $C.old; [ -d $C ] && mv $C $C.old; mv $T $C; rm -rf $C.old
fi
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
export HUNTER_JAVA_CMD="java -XX:+UseSerialGC -javaagent:$B/jars/entityagent.jar -javaagent:$C/delayagent.jar -javaagent:$C/hunteragent.jar $JVM_OPENS -Doptimus.graph.async.profiler=false -cp $C:$CP optimus.graph.Hunter"
export HUNTER_LOGDIR=${HUNTER_LOGDIR:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/hunter}
[ "$1" = "--print-cmd" ] && { echo "$HUNTER_JAVA_CMD"; exit 0; }
exec java ${JVM_EXTRA:-} ${HUNTER_OPTS:-} -cp "$C:$CP" optimus.graph.Hunter "$@"
