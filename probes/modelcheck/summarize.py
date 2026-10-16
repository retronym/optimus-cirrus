"""Print the RUN RESULTS section of a jcstress console log: per-test outcome tables across all
configurations, without the forked VMs' output/error streams."""
import re, sys
lines = [l.rstrip() for l in open(sys.argv[1], errors="replace") if "Entity Agent" not in l]
start = next((i for i, l in enumerate(lines) if l.startswith("RUN RESULTS")), len(lines))
keep = True
for l in lines[start:]:
    if re.match(r"\s+VM (output|error) stream:", l):
        keep = False
        continue
    if re.search(r"\[(OK|FAILED|ERROR)\] ", l) or re.match(r"\s+(Interesting|Failed|Error|All remaining) tests:", l) or l.startswith("  HTML report"):
        keep = True
    if l.startswith("Exception in thread") or l.startswith("\tat "):
        break
    if keep:
        print(l)
