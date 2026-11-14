"""跨进程后台领导锁：保证同一数据目录内全局只有一个后台 worker。

背景：Gunicorn 单 worker 平滑重载（SIGHUP / --max-requests 触发的
滚动重启）期间，新旧两个进程会在一段时间内并存并共用同一数据目录。
若每个进程都启动后台守护线程，就会出现：
- 新进程把仍存活的旧进程正在执行的 RUNNING 任务误标失败；
- 两个进程同时认领并执行同一个任务（重复导入/重复重算）；
- 旧进程迟到的发布会覆盖新进程刚发布的分组。

方案：在数据目录内对 ``worker.lock`` 持有一把进程级排他咨询锁
（``fcntl.flock``）。

- 只有持锁进程（leader）的后台线程可以认领/执行任务；未持锁进程
  仍然完整提供上传、核对、进度查询与对照下载等 Web 能力。
- ``flock`` 与持锁进程的生命周期绑定：进程正常退出时锁随描述符
  关闭而释放；进程被 SIGKILL/崩溃时由内核自动释放。进程被
  SIGSTOP 暂时暂停时锁 **不会** 释放，因此暂停不会触发误交接。
- 锁不可通过 fork 复制后在父子进程间共享：worker 线程在应用
  导入阶段才打开锁文件，且部署不使用 ``--preload``，因此每个
  Gunicorn 进程各自独立参与竞选。
"""
import errno
import fcntl
import os

from . import config


class LeaderLock:
    """对锁文件持有 LOCK_EX 的上下文管理器。

    锁在整个进程后台生命周期内保持；``acquire`` 采用非阻塞重试，
    重试间隙可被 stop_event 打断，从而保证进程收到退出信号时能
    及时结束等待，而不是卡在锁竞争上。
    """

    def __init__(self, path=None, poll_seconds=None, stop_event=None):
        self.path = path or config.LEADER_LOCK_PATH
        self.poll_seconds = (
            poll_seconds
            if poll_seconds is not None
            else config.LEADER_LOCK_POLL_SECONDS
        )
        self.stop_event = stop_event
        self._fh = None

    def acquire(self):
        # 锁文件所在目录必须已存在（config 启动时已创建数据目录）
        self._fh = open(self.path, "a+")
        try:
            while True:
                try:
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    return True
                except OSError as exc:
                    if exc.errno not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        raise
                if self.stop_event is not None and self.stop_event.wait(
                    self.poll_seconds
                ):
                    # 等待期间收到停止指令：放弃竞选并关闭描述符
                    self.release()
                    return False
        except Exception:
            self.release()
            raise

    def release(self):
        fh, self._fh = self._fh, None
        if fh is None:
            return
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        try:
            fh.close()
        except OSError:
            pass

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False
