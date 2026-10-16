package optimus.graph.tracking.ttracks;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import optimus.graph.NodeTask;

/**
 * W13 (Tier0Probes): the same traversal as Invalidators.invalidate (AllTTrackRefsGraphTraverser: invalidateCache on
 * every ref, then mark the track Invalid), additionally recording which tracks this traversal visited.
 */
public final class Tier0CountingInvalidator extends AllTTrackRefsGraphTraverser {
  public final Set<TTrack> visited = Collections.newSetFromMap(new IdentityHashMap<>());

  @Override
  protected void preVisit(TTrack ttrack) {
    visited.add(ttrack);
    super.preVisit(ttrack);
  }

  @Override
  protected void visitRef(NodeTask task, TTrackRef tref) {
    task.invalidateCache(null);
  }

  @Override
  protected void postVisit(TTrack ttrack) {
    ttrack.ptracks = Ptracks0.instance;
    ttrack.nodes = TTrackRef.Invalid;
  }
}
