#!/bin/bash
# Compile the cache model checks (W3, W11 jcstress; H1 Lincheck).
#   classes/fixtures   ProbeDag.java + CacheFixtures.scala from probes/cache/src (scalac mixed, then javac), our own copy
#   classes/jcstress   src/common + src/jcstress (jcstress annotation processor)
#   classes/lincheck   src/common + src/lincheck
source $(dirname $0)/env.sh || exit 1
SRC=$(cd $P/../src && pwd)
FIXSRC="$SRC/optimus/graph/ProbeDag.java $SRC/optimus/graph/CacheFixtures.scala"
rm -rf $P/classes; mkdir -p $P/classes/fixtures $P/classes/jcstress $P/classes/lincheck
java -Xss8m -cp "$(cat $B/cp.txt)" scala.tools.nsc.Main -nowarn -deprecation:false \
  -Yimports:java.lang,scala,scala.Predef,optimus.scala212.DefaultSeq \
  -cp "$CORE_CP" -d $FIX $FIXSRC || exit 1
javac -nowarn -proc:none -cp "$FIX:$CORE_CP" -d $FIX $SRC/optimus/graph/ProbeDag.java || exit 1
COMMON=$(find $P/src/common -name '*.java')
javac -nowarn -proc:full -processor org.openjdk.jcstress.infra.processors.JCStressTestProcessor \
  -cp "$JCSTRESS_CP:$FIX:$CORE_CP" -d $P/classes/jcstress $COMMON $(find $P/src/jcstress -name '*.java') || exit 1
javac -nowarn -proc:none -cp "$LINCHECK_CP:$FIX:$CORE_CP" -d $P/classes/lincheck $COMMON $(find $P/src/lincheck -name '*.java' 2>/dev/null) || exit 1
echo "built: $(find $P/classes -name '*.class' | wc -l) classes"
mkdir -p $P/classes/plain
javac -nowarn -proc:none -cp "$P/classes/jcstress:$FIX:$CORE_CP" -d $P/classes/plain $(find $P/src/plain -name '*.java') || exit 1
