package optimus.graph.cache;

import optimus.graph.NodeKey;
import optimus.graph.NodeTaskInfo;
import optimus.graph.OGSchedulerContext;
import optimus.graph.PropertyNode;
import optimus.platform.storable.Entity;

/**
 * A plain-Java cache key with the same identity as CacheMC.key (the fixture property's info, ProbeEntity(0), one int
 * arg) but no reference to the fixture model: Lincheck walks the object graph reachable from the test instance and
 * its statics, and cannot take field offsets of the fixture's record classes (ProbeDag.Key). Never run.
 */
public final class MCKey extends PropertyNode<Long> {
  static final NodeTaskInfo INFO = CacheMC.INFO;
  static final Entity ENTITY = CacheMC.SYS.entity(0);

  public final int arg, serial;

  public MCKey(int arg, int serial) {
    this.arg = arg;
    this.serial = serial;
    attach(CacheMC.ROOT);
  }

  @Override public Entity entity() { return ENTITY; }
  @Override public NodeTaskInfo propertyInfo() { return INFO; }
  @Override public Object[] args() { return new Object[] {arg}; }
  @Override public int argsHash() { return arg; }
  @Override public boolean argsEquals(NodeKey<?> that) { return that instanceof MCKey k && k.arg == arg; }
  @Override public void run(OGSchedulerContext ec) { throw new UnsupportedOperationException("cache key only"); }
  @Override public String toString() { return "k" + arg + "#" + serial; }
}
