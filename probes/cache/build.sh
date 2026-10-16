#!/bin/bash
# Build the Phase 2 cache-campaign probes into probes/cache/classes.
#   - scalac (2.13 from cp.txt) over every .scala under probes/cache/src, with the .java sources on its source path
#     for mixed compilation, then javac over every .java (probes/cache/src + the probes/src/probes harness files);
#   - probes/cache/classes/cacheagent.jar: the CacheDelayAgent (ASM hooks; ASM comes from the app class path).
# Env: CIRRUS_BUILD (core build dir), CACHE_EXCLUDE="path/suffix ..." (sources to leave out, e.g. a file another
# agent is mid-way through), CACHE_CLASSES (output dir, default probes/cache/classes).
P=$(cd $(dirname $0)/.. && pwd)          # probes/
C=$P/cache
B=${CIRRUS_BUILD:-/private/tmp/claude-501/-Users-jzaugg-virtus-code--worktrees-optimus-cirrus-cirrus-scheduler-review-faa430/b0bed4b8-87a5-47c8-b2ce-11e5e3ec21d5/scratchpad/build}
CP="$(cat $B/cp.txt)$(for d in $B/out/*/; do printf ":${d%/}"; done):$B/smoke/classes:$B/smoke/resources"
OUT=${CACHE_CLASSES:-$C/classes}
keep() { local f=$1; for x in ${CACHE_EXCLUDE:-}; do case $f in *$x) return 1;; esac; done; return 0; }
SCALA=(); JAVA=()
while IFS= read -r f; do keep "$f" && SCALA+=("$f"); done < <(find $C/src -name '*.scala' | sort)
while IFS= read -r f; do keep "$f" && JAVA+=("$f"); done < <(find $C/src -name '*.java' | sort)
JAVA+=($P/src/probes/Delay.java $P/src/probes/Steps.java)
T=$OUT.tmp.$$; rm -rf $T; mkdir -p $T
if [ ${#SCALA[@]} -gt 0 ]; then
  java -Xss8m -cp "$(cat $B/cp.txt)" scala.tools.nsc.Main -nowarn -deprecation:false \
    -Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq \
    -cp "$CP" -d $T "${SCALA[@]}" "${JAVA[@]}" || { rm -rf $T; exit 1; }
fi
javac -nowarn -proc:none -cp "$T:$CP" -d $T "${JAVA[@]}" || { rm -rf $T; exit 1; }
printf 'Premain-Class: probes.CacheDelayAgent\n' > $T/cacheagent.mf
(cd $T && jar cfm cacheagent.jar cacheagent.mf probes/CacheDelayAgent*.class) || { rm -rf $T; exit 1; }
rm -rf $OUT.old; [ -d $OUT ] && mv $OUT $OUT.old; mv $T $OUT; rm -rf $OUT.old
echo "built $OUT (${#SCALA[@]} scala, ${#JAVA[@]} java)"
