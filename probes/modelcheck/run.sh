#!/bin/bash
# Run the H1/H2 model checks.
#   run.sh jcstress [mode] [test-regexp] [extra jcstress args...]   mode: sanity|quick|default|tough|stress (default quick)
#   run.sh lincheck [-Dmc.*=...] [name=capacity:start ...]           see optimus.graph.H1Lincheck.main
#   run.sh h2e                                                       deterministic follow-up to H2 (e)
#   run.sh all                                                       jcstress quick on everything, then lincheck
source $(dirname $0)/env.sh || exit 1
what=${1:-all}; shift
run_jcstress() {
  local mode=${1:-quick}; local re=${2:-'optimus\.graph\.H[12].*'}; shift 2 2>/dev/null
  local out=$P/results/jcstress-$mode-$(date +%Y%m%d-%H%M%S)
  mkdir -p $out
  # The forked VMs inherit the host class path; the agent and module flags go in via -jvmArgsPrepend
  # so that jcstress still runs its usual set of JIT/GC configurations on top.
  (cd $out && java -cp "$P/classes/jcstress:$JCSTRESS_CP:$CORE_CP" org.openjdk.jcstress.Main \
    -m $mode -t "$re" -r $out/report \
    -jvmArgsPrepend "$AGENT $JVM_OPENS $GRAPH_PROPS -Dlog4j2.level=ERROR" "$@" 2>&1 | tee $out/console.txt)
  python3 -I $P/summarize.py $out/console.txt > $out/summary.txt; echo "results in $out (summary.txt)"
}
run_lincheck() {
  local props=() args=()
  for a in "$@"; do case $a in -D*|-X*) props+=("$a") ;; *) args+=("$a") ;; esac; done
  java "${props[@]}" $AGENT $JVM_OPENS $GRAPH_PROPS -Djdk.attach.allowAttachSelf=true -XX:+EnableDynamicAgentLoading \
    -cp "$P/classes/lincheck:$CORE_CP:$LINCHECK_CP" optimus.graph.H1Lincheck "${args[@]}" 2>&1 | tee $P/results/lincheck-$(date +%Y%m%d-%H%M%S).txt
}
mkdir -p $P/results
case $what in
  jcstress) run_jcstress "$@" ;;
  lincheck) run_lincheck "$@" ;;
  h2e) java $AGENT $JVM_OPENS $GRAPH_PROPS -cp "$P/classes/plain:$P/classes/jcstress:$CORE_CP" optimus.graph.H2eConsequence 2>&1 | grep -v "Entity Agent" ;;
  all) run_jcstress quick && run_lincheck ;;
  *) echo "usage: run.sh jcstress|lincheck|all ..."; exit 2 ;;
esac
