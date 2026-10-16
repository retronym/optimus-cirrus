#!/bin/bash
# Compile the H1/H2 model-checking tests. Output: classes/jcstress, classes/lincheck.
source $(dirname $0)/env.sh || exit 1
rm -rf $P/classes; mkdir -p $P/classes/jcstress $P/classes/lincheck
COMMON=$(find $P/src/common -name '*.java')
javac -nowarn -proc:full -processor org.openjdk.jcstress.infra.processors.JCStressTestProcessor \
  -cp "$JCSTRESS_CP:$CORE_CP" -d $P/classes/jcstress $COMMON $(find $P/src/jcstress -name '*.java') || exit 1
javac -nowarn -proc:none -cp "$LINCHECK_CP:$CORE_CP" -d $P/classes/lincheck $COMMON $(find $P/src/lincheck -name '*.java' 2>/dev/null) || exit 1
mkdir -p $P/classes/plain
javac -nowarn -proc:none -cp "$P/classes/jcstress:$CORE_CP" -d $P/classes/plain $(find $P/src/plain -name '*.java') || exit 1
echo "built: $(find $P/classes -name '*.class' | wc -l) classes"
