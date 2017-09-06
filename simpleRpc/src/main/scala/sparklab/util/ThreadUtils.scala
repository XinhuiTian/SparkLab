package sparklab.util

import java.util.concurrent._

import com.google.common.util.concurrent.{MoreExecutors, ThreadFactoryBuilder}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object ThreadUtils {
  private val sameThreadExecutionContext =
    ExecutionContext.fromExecutorService(MoreExecutors.sameThreadExecutor())

  /**
   * An `ExecutionContextExecutor` that runs each task in the thread that invokes `execute/submit`.
   * The caller should make sure the tasks running in this `ExecutionContextExecutor` are short and
   * never block.
   */
  def sameThread: ExecutionContextExecutor = sameThreadExecutionContext


  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory (prefix)
    Executors.newCachedThreadPool (threadFactory).asInstanceOf [ThreadPoolExecutor]
  }

  def newDaemonCachedThreadPool(
      prefix: String, maxThreadNumber: Int, keepAliveSeconds: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreadNumber, // corePoolSize: the max number of threads to create before queuing the tasks
      maxThreadNumber, // maximumPoolSize: because we use LinkedBlockingDeque, this one is not used
      keepAliveSeconds,
      TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable],
      threadFactory)
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon (true).setNameFormat (prefix + "-%d").build ()
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory (prefix)
    Executors.newFixedThreadPool (nThreads, threadFactory).asInstanceOf [ThreadPoolExecutor]
  }



}