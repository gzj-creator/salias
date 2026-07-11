/**
 * @file example/mpsc_publisher.cpp
 * @brief MPSC channel 示例：发布端进程。
 * @details 本示例演示如何以 producer 身份向一个具名
 *          MPSC（多生产者单消费者）channel 发布文本消息。
 *          它通过 common.hpp 中的 connect_with_retry 连接到由
 *          mpsc_subscriber 先创建的 channel（L7 公共 API），
 *          再用 publish_text 逐条发布。
 *          发布端不负责创建 channel，仅 connect；channel 必须先由订阅端 create，
 *          否则 connect 会持续重试直至超时。典型运行：
 *          先启动 mpsc_subscriber（创建者），再启动本程序（发布者）。
 */
#include <iostream>
#include <string>
#include <utility>

#include "common.hpp"

/**
 * @brief 发布端程序入口。
 * @param argc argv 元素总数。
 * @param argv 原始参数数组（支持 --name / --count / --message）。
 * @retval 0 成功发布全部消息。
 * @retval 1 连接或发布失败（channel 未就绪或不可恢复错误）。
 * @retval 2 命令行参数错误。
 */
int main(int argc, char** argv) {
  salias_example::Options options{
      .name = "salias-demo-mpsc",
      .message = "hello from mpsc publisher",
  };
  const salias_example::ParseSpec spec{.allow_message = true};
  const auto parsed = salias_example::parse_args(argc, argv, options, spec, std::cerr);
  if (parsed == salias_example::ParseStatus::Help) {
    salias_example::print_usage(std::cout, argv[0], spec);
    return 0;
  }
  if (parsed == salias_example::ParseStatus::Error) {
    salias_example::print_usage(std::cerr, argv[0], spec);
    return 2;
  }

  // 以 FifoMpsc 模式连接具名 channel；订阅端必须已先 create，否则重试至超时
  auto connected = salias_example::connect_with_retry<salias::Mode::FifoMpsc>(options.name);
  if (!connected) {
    std::cerr << "failed to connect MPSC channel: " << salias_example::error_name(connected.error())
              << '\n';
    return 1;
  }

  auto channel = std::move(connected).value();
  auto publisher = channel.publisher();
  // 循环发布 count 条带序号的消息（背压由 publish_text 内部重试消化）
  for (std::uint64_t i = 0; i < options.count; ++i) {
    const std::string payload = options.message + " #" + std::to_string(i);
    auto published = salias_example::publish_text(publisher, payload);
    if (!published) {
      std::cerr << "failed to publish MPSC message: "
                << salias_example::error_name(published.error()) << '\n';
      return 1;
    }
  }
  std::cout << "mpsc published count=" << options.count << '\n';
  return 0;
}
