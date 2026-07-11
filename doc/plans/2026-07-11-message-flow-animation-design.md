# salias 消息流动画设计

## 目标

提供一个无需构建、无需网络依赖的单文件 HTML 动画，解释 `Channel`、公共 API 层的
`Publisher` / `Subscriber`、底层 `Producer` / `Consumer`、per-producer `MagicRing`
之间的关系，并可切换观察四种通道模式的消息流转差异。

## 架构关系

- `Channel` 是具名 IPC 外观与共享状态容器，管理控制段、端点分配和多条 ring。
- 每个 `Publisher` 持有一个底层 Producer/Tx endpoint。
- 每个 Producer 独占一条 `MagicRing`，避免多个 Producer 竞争同一写入位置。
- `Subscriber` 持有 Consumer/Rx endpoint，轮询或归并所有 Producer ring。
- Fanout 模式为每个 Subscriber 保存独立的 per-producer 消费游标。
- `Subscriber::release()` 推进游标；release 前消息视图仍引用 ring 内存。

## 四种模式

### FifoMpsc

多条 ring 分别保持 producer 内 FIFO。单个 Consumer 轮询各 ring，跨 Producer 不承诺
全局顺序。

### FifoFanout

每个 Consumer 都读取每条 ring，并使用自己的独立游标。同一消息交付给所有 Subscriber，
最慢 Consumer 参与 Producer 的空间回收与背压判断。

### OrderedMpsc

Producer 发布前获取 `global_sequence`。Consumer 检查多条 ring 的头部 frame，按全局
sequence 归并；缺失下一序号时等待对应 Producer 发布。

### OrderedFanout

每个 Consumer 独立执行全局 sequence 归并，因此所有 Subscriber 均看到相同的全局有序流，
同时保留各自的消费进度。

## 动画表现

- 使用内联 SVG 绘制组件、连线、ring 槽位、游标与消息粒子。
- 顶部提供四模式切换、播放/暂停、单步、重播和速度控制。
- 中部展示 Publisher → Producer → frame encode → MagicRing → Consumer → Subscriber。
- 底部同步显示当前阶段说明、模式语义、图例和实现要点。
- 背压阶段展示慢 Consumer 阻止 ring 槽位回收；release 后恢复发布。
- 页面采用响应式深色技术风格，并兼容 `prefers-reduced-motion`。

## 文件与导航

- 新增 `doc/salias-message-flow.html`。
- 在 `README.md` 增加“架构动画”导航链接。

## 验证

- 检查 HTML 不引用外部脚本、样式或资源。
- 检查四个模式按钮、播放、暂停、单步、重播和速度控制均可工作。
- 检查窄屏下组件不重叠，文本仍可阅读。
- 检查 README 相对链接可以定位到动画文件。
