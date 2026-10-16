package probes;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import java.util.Map;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Cache-campaign delay-injection agent (same style as {@link DelayAgent} and HunterAgent). Rewrites the classes below
 * at load time to call {@code probes.CacheDelay.point(id)} (which counts, runs global handlers, then forwards to
 * {@code probes.Delay.point(id)}). Every insertion is a stack-neutral {@code ldc id; invokestatic}.
 *
 * <pre>
 *   NodeCacheBase.putIfAbsentCore     nc.put.head          after vh_table.getVolatile(t, i) (the bucket head read)
 *                                     nc.put.cas           immediately before vh_table.compareAndSet(t, i, head, ne)
 *   NodeCacheWithLRU.remove(NCEntryLRU) lru.remove         method entry
 *   NodeCacheWithLRU.catchupEntry     lru.catchup          method entry
 *   NodeCacheWithLRU.setMaxSize       lru.setMaxSize.swapped  after "table = new NCEntryLRU[..]" (before moveEntries)
 *   TTrack.nodeCompleted              ttrack.completed.cas before nodes_h.weakCompareAndSet (each retry)
 *   TTrack.addCaller(TTrack,int)      ttrack.addCaller     method entry
 *   TTrack.merge                      ttrack.merge         method entry
 *   PtracksArray.addCaller            ptracks.nextFree     after each read of this.nextFree
 *                                     ptracks.resize.cas   before data_h.compareAndSet(this, data, newData)
 *                                     ptracks.slot.cas     before dataElm_h.compareAndSet(data, nextFree, null, c)
 *   DelayedXSFTProxyNode.runByWaitingOrEvaluating
 *                                     xsft.promised        after upProxy.promiseOrAwait(this) returns
 *                                     xsft.beforePromisedToUp  before "cips.promisedToUp = upProxy"
 *   DelayedXSFTProxyNode.offerValue   xsft.offer.beforeSync    before synchronized(this)
 *   DelayedXSProxyNode.run            xs.run               method entry
 *   DependencyTrackerQueue.scheduleAction  dtq.schedule    method entry
 *   DependencyTrackerQueue.evaluationBarrier (its foreach lambda)
 *                                     dtq.barrier.beforeWait   after cancelInternal, before scheduler.waitForCancelledNodesToStop
 *   DependencyTrackerQueue.cancelOrphanedNodesAndResetCancellationScope (its foreach lambda)
 *                                     dtq.cancelOrphans.beforeWait  ditto
 *   OGScheduler.waitForCancelledNodeToStop  sched.waitCancelled.loop  top of the polling loop body (each iteration)
 * </pre>
 *
 * Prints the inserted hooks per class at load time ("CacheDelayAgent: instrumented ...").
 */
public final class CacheDelayAgent {
  static final String HOOK = "probes/CacheDelay";
  static final String VH = "java/lang/invoke/VarHandle";

  static final Map<String, String> TARGETS =
      Map.of(
          "optimus/graph/cache/NodeCacheBase", "NodeCacheBase",
          "optimus/graph/cache/NodeCacheWithLRU", "NodeCacheWithLRU",
          "optimus/graph/tracking/ttracks/TTrack", "TTrack",
          "optimus/graph/tracking/ttracks/PtracksArray", "PtracksArray",
          "optimus/graph/cache/DelayedXSFTProxyNode", "DelayedXSFTProxyNode",
          "optimus/graph/cache/DelayedXSProxyNode", "DelayedXSProxyNode",
          "optimus/graph/tracking/DependencyTrackerQueue", "DependencyTrackerQueue",
          "optimus/graph/OGScheduler", "OGScheduler");

  public static void premain(String args, Instrumentation inst) {
    inst.addTransformer(
        new ClassFileTransformer() {
          @Override
          public byte[] transform(
              ClassLoader loader, String name, Class<?> cls, ProtectionDomain pd, byte[] bytes) {
            if (name == null || !TARGETS.containsKey(name) || cls != null) return null;
            try {
              return instrument(name, bytes);
            } catch (Throwable t) {
              System.err.println("CacheDelayAgent: failed to instrument " + name + ": " + t);
              t.printStackTrace();
              return null;
            }
          }
        });
  }

  static byte[] instrument(String cname, byte[] bytes) {
    ClassReader cr = new ClassReader(bytes);
    ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
    StringBuilder log = new StringBuilder();
    String simple = TARGETS.get(cname);
    cr.accept(
        new ClassVisitor(Opcodes.ASM9, cw) {
          @Override
          public MethodVisitor visitMethod(int access, String m, String desc, String sig, String[] exc) {
            MethodVisitor mv = super.visitMethod(access, m, desc, sig, exc);
            if ((access & Opcodes.ACC_BRIDGE) != 0) return mv; // (Scala lambda bodies are synthetic: keep them)
            return new Hooker(mv, simple, m, desc, log);
          }
        },
        0);
    System.err.println("CacheDelayAgent: instrumented " + cname + ":" + log);
    return cw.toByteArray();
  }

  static final class Hooker extends MethodVisitor {
    final String c, m, desc;
    final StringBuilder log;

    Hooker(MethodVisitor mv, String c, String m, String desc, StringBuilder log) {
      super(Opcodes.ASM9, mv);
      this.c = c;
      this.m = m;
      this.desc = desc;
      this.log = log;
    }

    boolean in(String cls, String meth) {
      return c.equals(cls) && m.equals(meth);
    }

    void hook(String id) {
      super.visitLdcInsn(id);
      super.visitMethodInsn(Opcodes.INVOKESTATIC, HOOK, "point", "(Ljava/lang/String;)V", false);
      String rec = c + "." + m + ":" + id;
      if (CacheDelay.instrumented.add(rec)) log.append(' ').append(m).append(':').append(id);
    }

    @Override
    public void visitCode() {
      super.visitCode();
      if (in("NodeCacheWithLRU", "remove") && desc.equals("(Loptimus/graph/cache/NCEntryLRU;)V")) hook("lru.remove");
      if (in("NodeCacheWithLRU", "catchupEntry")) hook("lru.catchup");
      if (in("TTrack", "addCaller") && desc.equals("(Loptimus/graph/tracking/ttracks/TTrack;I)V"))
        hook("ttrack.addCaller");
      if (in("TTrack", "merge")) hook("ttrack.merge");
      if (in("DelayedXSProxyNode", "run")) hook("xs.run");
      if (c.equals("DependencyTrackerQueue") && (m.equals("scheduleAction") || m.endsWith("$$scheduleAction")))
        hook("dtq.schedule");
    }

    @Override
    public void visitMethodInsn(int op, String owner, String name, String d, boolean itf) {
      boolean vh = owner.equals(VH);
      if (vh && in("NodeCacheBase", "putIfAbsentCore") && name.equals("compareAndSet")) hook("nc.put.cas");
      if (vh && in("TTrack", "nodeCompleted") && name.equals("weakCompareAndSet")) hook("ttrack.completed.cas");
      if (vh && in("PtracksArray", "addCaller") && name.equals("compareAndSet"))
        hook(d.contains("I") ? "ptracks.slot.cas" : "ptracks.resize.cas");
      if (name.equals("waitForCancelledNodesToStop") && c.equals("DependencyTrackerQueue")) {
        if (m.contains("evaluationBarrier")) hook("dtq.barrier.beforeWait");
        else if (m.contains("cancelOrphanedNodesAndResetCancellationScope")) hook("dtq.cancelOrphans.beforeWait");
      }
      super.visitMethodInsn(op, owner, name, d, itf);
      if (vh && in("NodeCacheBase", "putIfAbsentCore") && name.equals("getVolatile")) hook("nc.put.head");
      if (in("DelayedXSFTProxyNode", "runByWaitingOrEvaluating") && name.equals("promiseOrAwait"))
        hook("xsft.promised");
    }

    @Override
    public void visitFieldInsn(int op, String owner, String name, String d) {
      if (op == Opcodes.PUTFIELD && in("DelayedXSFTProxyNode", "runByWaitingOrEvaluating") && name.equals("promisedToUp"))
        hook("xsft.beforePromisedToUp");
      super.visitFieldInsn(op, owner, name, d);
      if (op == Opcodes.PUTFIELD && in("NodeCacheWithLRU", "setMaxSize") && name.equals("table"))
        hook("lru.setMaxSize.swapped");
      if (op == Opcodes.GETFIELD && in("PtracksArray", "addCaller") && name.equals("nextFree"))
        hook("ptracks.nextFree");
    }

    @Override
    public void visitTypeInsn(int op, String type) {
      if (op == Opcodes.INSTANCEOF && in("OGScheduler", "waitForCancelledNodeToStop")
          && type.equals("optimus/graph/OGSchedulerContext$SyncFrame"))
        hook("sched.waitCancelled.loop");
      super.visitTypeInsn(op, type);
    }

    @Override
    public void visitInsn(int op) {
      if (op == Opcodes.MONITORENTER && in("DelayedXSFTProxyNode", "offerValue")) hook("xsft.offer.beforeSync");
      super.visitInsn(op);
    }
  }
}
