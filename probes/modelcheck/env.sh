# Sourced by build.sh / run.sh. Classpaths and JVM flags shared by jcstress and Lincheck runs.
P=$(cd $(dirname ${BASH_SOURCE[0]}) && pwd)
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
[ -f $P/jcstress.cp ] && [ -f $P/lincheck.cp ] || $P/fetch.sh || exit 1
CORE_CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
JCSTRESS_CP="$(cat $P/jcstress.cp)"
LINCHECK_CP="$(cat $P/lincheck.cp)"
AGENT="-javaagent:$B/jars/entityagent.jar"
JVM_OPENS="--add-exports java.base/jdk.internal.vm=ALL-UNNAMED --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.monitor.event=ALL-UNNAMED --add-opens jdk.internal.jvmstat/sun.jvmstat.perfdata.monitor.protocol.local=ALL-UNNAMED --add-opens jdk.attach/sun.tools.attach=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
GRAPH_PROPS="-Doptimus.graph.async.profiler=false"
