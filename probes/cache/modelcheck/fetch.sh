#!/bin/bash
# Fetch jcstress and Lincheck with coursier; writes jcstress.cp and lincheck.cp next to this script.
P=$(cd $(dirname $0) && pwd)
coursier fetch -q --classpath org.openjdk.jcstress:jcstress-core:${JCSTRESS_VERSION:-0.16} > $P/jcstress.cp || exit 1
coursier fetch -q --classpath org.jetbrains.kotlinx:lincheck-jvm:${LINCHECK_VERSION:-2.39} > $P/lincheck.cp || exit 1
echo "jcstress: $(tr ':' '\n' < $P/jcstress.cp | wc -l) jars; lincheck: $(tr ':' '\n' < $P/lincheck.cp | wc -l) jars"
