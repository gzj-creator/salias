# 项目布局

```text
src/core/
├── platform/   mapping、map_options、platform error
├── ring/       magic_ring、atomic_cell
├── frame/      header、codec、sequence
├── flow/       producer、consumer、position
├── wait/       wait_strategy、spin_pause、cpu_relax
└── channel/    hybrid_control、hybrid_mpsc、shared_hybrid_mpsc

src/salias/
├── include/salias/  Channel、Publisher、Subscriber、Config、Message、Error
└── src/channel.cpp  具名 IPC 控制块、四 Mode facade、持久端点

test/       platform、ring、frame、flow、hybrid channel、API
bench/      channel_stress、salias_ipc_compare
example/    FIFO MPSC publisher/subscriber
tools/      Aeron compare harness
```

依赖方向：

```text
salias facade -> channel -> flow -> frame -> ring -> platform
                         \-> wait
```

仓库不再包含单环 SPSC/MPSC/MPMC 通道、metrics 子系统、Futex wait 后端或冗余 smoke benchmark。
