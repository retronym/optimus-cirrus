#!/bin/bash
# Run the cache model checks.
#   run.sh jcstress [mode] [test-regexp] [extra jcstress args...]   mode: sanity|quick|default|tough|stress (default quick)
#   run.sh lincheck [-Dmc.*=...] [config ...]                       see optimus.graph.cache.H1CacheLincheck.main
#   run.sh shards N [-Dmc.iterations=100] [-Dname=tag] [-Dmc.*=...]   N parallel Lincheck JVMs splitting the scenarios
#   run.sh smoke                                                    sequential sanity of the CacheMC helpers
source $(dirname $0)/env.sh || exit 1
what=${1:-smoke}; shift
run_jcstress() {
  local mode=${1:-quick}; local re=${2:-'optimus\.graph\.(cache\.W3|tracking\.ttracks\.W11).*'}; shift 2 2>/dev/null
  local out=$P/results/jcstress-$mode-$(date +%Y%m%d-%H%M%S)
  mkdir -p $out
  # Forked VMs inherit the host class path; the agent and module flags go in via -jvmArgsPrepend so that jcstress
  # still runs its usual set of JIT/GC configurations on top.
  (cd $out && java -cp "$P/classes/jcstress:$FIX:$JCSTRESS_CP:$CORE_CP" org.openjdk.jcstress.Main \
    -m $mode -t "$re" -r $out/report \
    -jvmArgsPrepend "$AGENT $JVM_OPENS $GRAPH_PROPS -Dlog4j2.level=ERROR" "$@" 2>&1 | tee $out/console.txt)
  python3 -I $P/summarize.py $out/console.txt > $out/summary.txt; echo "results in $out (summary.txt)"
}
run_lincheck() {
  local props=() args=() log=$P/results/lincheck-${TAG:-$(date +%Y%m%d-%H%M%S)}.txt
  for a in "$@"; do case $a in -D*|-X*) props+=("$a") ;; *) args+=("$a") ;; esac; done
  java -Xmx3g -Dlincheck.instrumentAllClasses=true "${props[@]}" $AGENT $JVM_OPENS $GRAPH_PROPS -Djdk.attach.allowAttachSelf=true -XX:+EnableDynamicAgentLoading \
    -cp "$P/classes/lincheck:$FIX:$CORE_CP:$LINCHECK_CP" optimus.graph.cache.H1CacheLincheck "${args[@]}" > $log 2>&1
  local rc=$?; grep "^===" $log | tail -3; echo "log: $log"; return $rc
}
# N JVMs in parallel, seeds 1..N, each with 100/N generated scenarios (mc.iterations total, default 100).
run_shards() {
  local n=$1; shift
  local props=() total=100 name=shards
  for a in "$@"; do case $a in -Dmc.iterations=*) total=${a#*=} ;; -Dname=*) name=${a#*=} ;; *) props+=("$a") ;; esac; done
  local per=$(( (total + n - 1) / n ))
  for i in $(seq 1 $n); do
    TAG=$name-$i run_lincheck "${props[@]}" -Dmc.seed=$i -Dmc.iterations=$per -Dmc.handwritten=$([ $i = 1 ] && echo true || echo false) &
  done
  wait
  grep -h "^=== " $P/results/lincheck-$name-*.txt
}
mkdir -p $P/results
case $what in
  jcstress) run_jcstress "$@" ;;
  lincheck) run_lincheck "$@" ;;
  shards) run_shards "$@" ;;
  smoke) java $AGENT $JVM_OPENS $GRAPH_PROPS -cp "$P/classes/plain:$P/classes/jcstress:$FIX:$CORE_CP" optimus.graph.cache.CacheMCSmoke 2>&1 | grep -v "Entity Agent" ;;
  *) echo "usage: run.sh jcstress|lincheck|smoke ..."; exit 2 ;;
esac
