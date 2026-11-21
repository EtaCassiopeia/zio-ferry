package zio

/**
 * The one place the simulator reaches inside ZIO: a runtime whose fibers all run on a given executor, blocking ones
 * included, built without running any effect.
 *
 * The public way, `Runtime.unsafe.fromLayer(Runtime.setExecutor(e))`, builds the runtime by running a layer, and that
 * layer moves the very fiber building it onto `e`; when `e` is the simulator, which nobody is driving yet, the build
 * can wait forever. Setting the executor `FiberRef`s directly avoids running anything. They are `private[zio]`, hence
 * this file's package.
 */
object SimulatorRuntime:

  def on(executor: Executor): Runtime[Any] =
    val id   = FiberId.Runtime(0, 0L, Trace.empty)
    val refs = FiberRefs.empty
      .updatedAs(id)(FiberRef.overrideExecutor, Some(executor))
      .updatedAs(id)(FiberRef.currentBlockingExecutor, executor)
    Runtime(ZEnvironment.empty, refs, RuntimeFlags.default)
