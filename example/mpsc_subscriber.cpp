/**
 * @file example/mpsc_subscriber.cpp
 * @brief MPSC channel 示例：订阅端进程（channel 创建者）。
 * @details 本示例以 consumer 身份创建一个具名
 *          MPSC（多生产者单消费者）channel，
 *          并以忙等轮询方式消费文本消息。
 *          它负责 create channel（L7 公共 API），
 *          因此必须先于 mpsc_publisher 启动。
 *          create 时通过 salias::Config 指定 name / mode / capacity，
 *          其中 capacity 决定底层 ring 的字节容量；容量过小会导致
 *          生产端频繁遇到背压（BackPressured）。典型运行：
 *          先启动本程序（创建 + 订阅），
 *          再启动 mpsc_publisher（连接 + 发布）。
 */
#include <iostream>
#include <utility>

#include "common.hpp"

/**
 * @brief 订阅端程序入口。
 * @param argc argv 元素总数。
 * @param argv 原始参数数组（支持 --name / --count / --capacity）。
 * @retval 0 成功接收全部消息。
 * @retval 1 channel 创建失败（如共享内存已存在且配置冲突、平台失败）。
 * @retval 2 命令行参数错误。
 */
int main(int argc, char** argv) {
  salias_example::Options options{
      .name = "salias-demo-mpsc",
      .message = "",
  };
  const salias_example::ParseSpec spec{.allow_capacity = true};
  const auto parsed = salias_example::parse_args(argc, argv, options, spec, std::cerr);
  if (parsed == salias_example::ParseStatus::Help) {
    salias_example::print_usage(std::cout, argv[0], spec);
    return 0;
  }
  if (parsed == salias_example::ParseStatus::Error) {
    salias_example::print_usage(std::cerr, argv[0], spec);
    return 2;
  }

  // 构造 channel 配置：具名键、模式、ring 容量
  salias::Config config;
  config.name = options.name;
  config.mode = salias::Mode::FifoMpsc;
  config.capacity = options.capacity;

  // 以创建者身份初始化 channel（分配共享内存 + ring）
  // 同名 channel 已存在时会返回错误
  auto created = salias::FifoMpscChannel::create(config);
  if (!created) {
    std::cerr << "failed to create MPSC channel: " << salias_example::error_name(created.error())
              << '\n';
    return 1;
  }

  auto channel = std::move(created).value();
  auto subscriber = channel.subscriber();
  // 打印就绪信号并 flush，确保发布端在重试连接前能观测到订阅端已就绪
  std::cout << "mpsc subscriber ready name=" << options.name << " count=" << options.count << '\n';
  std::cout.flush();
  salias_example::receive_messages(subscriber, options.count, "mpsc");
  return 0;
}
