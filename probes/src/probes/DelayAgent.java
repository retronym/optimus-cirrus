package probes;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * P3 delay-injection agent. Rewrites optimus.graph.OGSchedulerContext$WorkQueue at load time to call
 * {@code probes.Delay.point(id)} at these points (all stack-neutral insertions):
 *
 * <pre>
 *   steal.read          tryToSteal:     after VarHandle.getVolatile(q, i), before the CAS (inside synchronized)
 *   pop.read            localPop:       after VarHandle.get(q, i), before the CAS
 *   tryLocalPop.read    tryLocalPop:    after VarHandle.get(q, i), before the CAS
 *   grow.read           growArray:      after each oq[i &amp; omask] read in the copy loop
 *   grow.beforeSwap     growArray:      after the copy loop, before synchronized(this) { _base = ..; _q = .. }
 *   scan.read           scanToFindWork: after the unsynchronized q[scanTop &amp; mask] read
 *   scan.beforeRemove   scanToFindWork: immediately before each removeAt(scanTop) call
 * </pre>
 *
 * Uses the ASM 9 on the application class path (org.ow2.asm). Independent of entityagent; chained
 * after it when given as a second -javaagent.
 */
public final class DelayAgent {
  static final String TARGET = "optimus/graph/OGSchedulerContext$WorkQueue";
  static final String HOOK = "probes/Delay";

  public static void premain(String args, Instrumentation inst) {
    inst.addTransformer(
        new ClassFileTransformer() {
          @Override
          public byte[] transform(
              ClassLoader loader, String name, Class<?> cls, ProtectionDomain pd, byte[] bytes) {
            if (!TARGET.equals(name)) return null;
            try {
              return instrument(bytes);
            } catch (Throwable t) {
              System.err.println("DelayAgent: failed to instrument " + name + ": " + t);
              t.printStackTrace();
              return null;
            }
          }
        });
  }

  static byte[] instrument(byte[] bytes) {
    ClassReader cr = new ClassReader(bytes);
    ClassWriter cw = new ClassWriter(cr, ClassWriter.COMPUTE_MAXS);
    StringBuilder log = new StringBuilder();
    cr.accept(
        new ClassVisitor(Opcodes.ASM9, cw) {
          @Override
          public MethodVisitor visitMethod(
              int access, String mname, String desc, String sig, String[] exc) {
            MethodVisitor mv = super.visitMethod(access, mname, desc, sig, exc);
            return switch (mname) {
              case "tryToSteal", "localPop", "tryLocalPop", "growArray", "scanToFindWork" ->
                  new Hooker(mv, mname, log);
              default -> mv;
            };
          }
        },
        0);
    System.err.println("DelayAgent: instrumented " + TARGET + ":" + log);
    return cw.toByteArray();
  }

  static final class Hooker extends MethodVisitor {
    final String m;
    final StringBuilder log;

    Hooker(MethodVisitor mv, String m, StringBuilder log) {
      super(Opcodes.ASM9, mv);
      this.m = m;
      this.log = log;
    }

    void hook(String id) {
      super.visitLdcInsn(id);
      super.visitMethodInsn(Opcodes.INVOKESTATIC, HOOK, "point", "(Ljava/lang/String;)V", false);
      log.append(' ').append(m).append(':').append(id);
      Delay.recordInstrumented(m + ":" + id);
    }

    @Override
    public void visitMethodInsn(int op, String owner, String name, String desc, boolean itf) {
      boolean vh = owner.equals("java/lang/invoke/VarHandle");
      if (m.equals("scanToFindWork") && name.equals("removeAt")) hook("scan.beforeRemove");
      super.visitMethodInsn(op, owner, name, desc, itf);
      if (vh && m.equals("tryToSteal") && name.equals("getVolatile")) hook("steal.read");
      if (vh && m.equals("localPop") && name.equals("get")) hook("pop.read");
      if (vh && m.equals("tryLocalPop") && name.equals("get")) hook("tryLocalPop.read");
    }

    @Override
    public void visitInsn(int op) {
      if (op == Opcodes.MONITORENTER && m.equals("growArray")) hook("grow.beforeSwap");
      super.visitInsn(op);
      if (op == Opcodes.AALOAD && m.equals("growArray")) hook("grow.read");
      if (op == Opcodes.AALOAD && m.equals("scanToFindWork")) hook("scan.read");
    }
  }
}
