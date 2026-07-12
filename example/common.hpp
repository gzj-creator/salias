/**
 * @file example/common.hpp
 * @brief 示例程序共享的工具与类型定义。
 * @details 本头文件为 example/ 目录下各示例（如 mpsc_publisher、mpsc_subscriber）
 *          提供公共基础设施：命令行参数解析、salias 公共 API 的便利封装
 *          （带重试的连接、带背压重试的发布、轮询消费循环）。
 *          它位于分层架构的 L7 salias 公共 API 之上，仅依赖
 *          salias::Channel / Publisher / Subscriber 等 L7 接口，
 *          不直接触碰底层 mmap / ring / frame 等机制。
 *          所有类型位于 namespace salias_example 中，仅供示例代码使用，
 *          非库对外 API 的一部分。
 */
#pragma once

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <iostream>
#include <salias/salias.hpp>
#include <span>
#include <string>
#include <string_view>
#include <thread>

/// 示例程序专用命名空间，封装命令行解析与 salias API 的便利封装，
/// 与库本身的 namespace salias 隔离，避免污染公共 API。
namespace salias_example {

/**
 * @brief 命令行解析后的运行期选项。
 * @details 汇总各示例程序的公共可配置项。不同示例通过
 *          ParseSpec 选择性启用部分字段。
 *          @a name 用于标识共享内存 channel 的具名键（跨进程寻址）；
 *          @a capacity 为 ring 的容量（字节数）；@a create 决定是否以创建者身份
 *          初始化 channel。
 */
struct Options {
  std::string name;
  std::uint64_t count = 10;
  std::size_t capacity = 1u << 20;
  std::string message;
  bool create = false;
};

/**
 * @brief 描述当前示例程序接受哪些命令行选项。
 * @details 用于在 print_usage 与 parse_args 中按需开启
 *          --create / --capacity / --message 三类选项，
 *          使同一套解析逻辑可复用于发布端与订阅端等不同角色。
 */
struct ParseSpec {
  bool allow_create = false;
  bool allow_capacity = false;
  bool allow_message = false;
};

/// 命令行解析结果状态：Ok 正常、Help 请求帮助、Error 解析失败。
enum class ParseStatus { Ok, Help, Error };

/**
 * @brief 将 salias::Error 枚举转为人类可读的 C 字符串。
 * @param error 待转换的错误码。
 * @return 指向字符串字面量的指针，生命周期为程序全程
 *         （noexcept、无内存分配）。
 * @note 用于在示例的 stderr 输出中打印错误名，便于诊断。
 */
inline const char* error_name(salias::Error error) noexcept {
  switch (error) {
    case salias::Error::Ok:
      return "Ok";
    case salias::Error::NotFound:
      return "NotFound";
    case salias::Error::VersionMismatch:
      return "VersionMismatch";
    case salias::Error::BackPressured:
      return "BackPressured";
    case salias::Error::MessageTooLarge:
      return "MessageTooLarge";
    case salias::Error::Lagged:
      return "Lagged";
    case salias::Error::PlatformFail:
      return "PlatformFail";
    case salias::Error::BadConfig:
      return "BadConfig";
    case salias::Error::OutOfMemory:
      return "OutOfMemory";
  }
  return "Unknown";
}

/**
 * @brief 将字符串解析为无符号整数。
 * @tparam UInt 目标无符号整数类型（如 std::uint64_t、std::size_t）。
 * @param text 待解析的文本视图，必须全部为数字字符。
 * @param out [out] 解析成功时写入结果；失败时不修改。
 * @retval true 整串均为合法数字且无溢出。
 * @retval false 含非法字符、溢出或整串未完全消费。
 * @note 使用 std::from_chars（无内存分配、无异常、无 locale 依赖），
 *       性能优于 strtoul。
 */
template <class UInt>
bool parse_unsigned(std::string_view text, UInt& out) noexcept {
  UInt value{};
  const char* const first = text.data();
  const char* const last = text.data() + text.size();
  const auto [ptr, ec] = std::from_chars(first, last, value);
  if (ec != std::errc{} || ptr != last) {
    return false;
  }
  out = value;
  return true;
}

/**
 * @brief 为需要取值的命令行选项读取下一个 argv 元素。
 * @param index [in,out] 当前 argv 游标，成功时自增指向已消费的值。
 * @param argc argv 元素总数。
 * @param argv 原始参数数组。
 * @param option 选项名（仅用于错误提示）。
 * @param value [out] 成功时写入该选项的取值。
 * @param err 错误信息的输出流。
 * @retval true 已成功读取取值。
 * @retval false 该选项缺少取值（已到达 argv 末尾）。
 */
inline bool require_value(int& index, int argc, char** argv, std::string_view option,
                          std::string_view& value, std::ostream& err) {
  if (index + 1 >= argc) {
    err << "missing value for " << option << '\n';
    return false;
  }
  value = argv[++index];
  return true;
}

/**
 * @brief 打印用法说明。
 * @param out 输出流。
 * @param program 程序名（通常取 argv[0]）。
 * @param spec 控制是否展示 --capacity / --message / --create 等可选项。
 */
inline void print_usage(std::ostream& out, const char* program, ParseSpec spec) {
  out << "usage: " << program << " [--name NAME] [--count N]";
  if (spec.allow_capacity) {
    out << " [--capacity BYTES]";
  }
  if (spec.allow_message) {
    out << " [--message TEXT]";
  }
  if (spec.allow_create) {
    out << " [--create]";
  }
  out << '\n';
}

/**
 * @brief 解析命令行参数到 Options。
 * @param argc argv 元素总数。
 * @param argv 原始参数数组。
 * @param options [out] 解析结果写入此结构体。
 * @param spec 声明本示例接受哪些选项。
 * @param err 错误信息输出流。
 * @retval ParseStatus::Ok 全部参数解析成功。
 * @retval ParseStatus::Help 调用方请求 --help / -h。
 * @retval ParseStatus::Error 遇到未知参数、缺值或非法取值。
 * @note 遇到首个错误即返回，不做错误恢复。
 */
inline ParseStatus parse_args(int argc, char** argv, Options& options, ParseSpec spec,
                              std::ostream& err) {
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    std::string_view value;
    if (arg == "--help" || arg == "-h") {
      return ParseStatus::Help;
    }
    if (arg == "--name") {
      if (!require_value(i, argc, argv, arg, value, err)) {
        return ParseStatus::Error;
      }
      options.name = value;
    } else if (arg == "--count") {
      // count 必须 > 0：发送/接收 0 条消息的示例无意义
      if (!require_value(i, argc, argv, arg, value, err) || !parse_unsigned(value, options.count) ||
          options.count == 0) {
        err << "invalid --count value\n";
        return ParseStatus::Error;
      }
    } else if (arg == "--capacity" && spec.allow_capacity) {
      // capacity 必须 > 0：ring 容量为 0 无意义且会导致下游断言失败
      if (!require_value(i, argc, argv, arg, value, err) ||
          !parse_unsigned(value, options.capacity) || options.capacity == 0) {
        err << "invalid --capacity value\n";
        return ParseStatus::Error;
      }
    } else if (arg == "--message" && spec.allow_message) {
      if (!require_value(i, argc, argv, arg, value, err)) {
        return ParseStatus::Error;
      }
      options.message = value;
    } else if (arg == "--create" && spec.allow_create) {
      options.create = true;
    } else {
      err << "unknown argument: " << arg << '\n';
      return ParseStatus::Error;
    }
  }
  return ParseStatus::Ok;
}

/**
 * @brief 以轮询重试方式连接具名 channel。
 * @tparam M channel 模式（如 Mode::FifoMpsc），决定底层 ring 的并发模型。
 * @param name channel 的具名键，对应共享内存中的寻址键。
 * @param timeout 最长等待时长，默认 5 秒。典型场景是订阅端先 create、
 *                发布端稍后 connect，二者启动顺序不保证，故需重试。
 * @return 成功返回 channel 句柄；失败返回最后一次错误码。
 * @retval salias::Error::NotFound 超时仍未找到对应 channel（创建端尚未就绪）。
 * @retval 其它非 NotFound 错误 立即返回，不再重试。
 * @note 仅对 NotFound（创建端尚未就绪）进行重试，其它错误立即向上传播。
 *       重试间调用 std::this_thread::yield() 让出 CPU，避免空转占用核心。
 */
template <salias::Mode M>
salias::Result<salias::Channel<M>> connect_with_retry(
    std::string_view name, std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  salias::Error last_error = salias::Error::NotFound;
  while (std::chrono::steady_clock::now() < deadline) {
    auto connected = salias::Channel<M>::connect(name);
    if (connected) {
      return connected;
    }
    last_error = connected.error();
    if (last_error != salias::Error::NotFound) {
      // 非 NotFound 错误（如版本不匹配、平台失败）不可重试解决，立即返回
      return std::unexpected(last_error);
    }
    // 创建端尚未就绪，短暂让出 CPU 后再试
    std::this_thread::yield();
  }
  return std::unexpected(last_error);
}

/**
 * @brief 发布一条文本消息，遇背压自动重试。
 * @tparam M channel 模式。
 * @param publisher 发布者句柄。
 * @param text 待发布的文本（不含终止 '\0'）。
 * @return 成功返回 true；失败返回无法重试的错误码。
 * @retval true 已成功 claim + 写入 payload + commit。
 * @retval salias::Error::MessageTooLarge 文本超出 ring 单帧容量上限，无法发布。
 * @retval 其它非 BackPressured 错误 立即返回。
 * @note claim/commit 两阶段写入：先申请一段 payload 空间，写入后再提交，
 *       提交前消费者不可见该帧。流控窗口满时返回 BackPressured，
 *       本函数通过 yield + 重试实现简单背压适应。
 */
template <salias::Mode M>
salias::Result<bool> publish_text(salias::Publisher<M>& publisher, std::string_view text) {
  for (;;) {
    auto claimed = publisher.try_claim(text.size());
    if (claimed) {
      auto claim = std::move(claimed).value();
      std::memcpy(claim.payload().data(), text.data(), text.size());
      // commit 之前的 acquire/release 语义确保消费者读到完整帧
      claim.commit();
      return true;
    }
    if (claimed.error() != salias::Error::BackPressured) {
      // 非背压错误（如 MessageTooLarge）不可通过重试解决，立即返回
      return std::unexpected(claimed.error());
    }
    // 流控窗口已满（背压），让出 CPU 等待消费者推进 position 后重试
    std::this_thread::yield();
  }
}

/**
 * @brief 轮询消费回调的用户上下文。
 * @details 通过 void* 透传给 poll 回调，累加已收到的消息数
 *          并携带输出标签前缀。
 *          @a label 用于在打印时区分不同订阅者。
 */
struct PollContext {
  const char* label = "";
  std::uint64_t received = 0;
};

/**
 * @brief 轮询回调：打印每条消息并累加计数。
 * @param message 从 ring 中读出的帧（payload 为字节视图）。
 * @param user 调用方透传的 PollContext 指针。
 * @note noexcept：回调不可抛异常，否则会中断 poll 循环。
 *       使用 reinterpret_cast 将字节 payload 当作 char 输出（文本协议）。
 */
inline void print_message_callback(const salias::Message& message, void* user) noexcept {
  auto* context = static_cast<PollContext*>(user);
  ++context->received;
  std::cout << context->label << " received[" << context->received << "]: ";
  std::cout.write(reinterpret_cast<const char*>(message.payload.data()),
                  static_cast<std::streamsize>(message.payload.size()));
  std::cout << '\n';
  std::cout.flush();
}

/**
 * @brief 同步接收并打印 count 条消息。
 * @tparam M channel 模式。
 * @param subscriber 订阅者句柄。
 * @param count 期望接收的消息条数，达到后即返回。
 * @param label 打印前缀，用于区分不同订阅者输出。
 * @note 采用批量轮询（每次最多 64 帧）+ 无消息时 yield 的忙等方式，
 *       属于最简单的 spin wait strategy。生产环境可替换为更高效的等待策略。
 */
template <salias::Mode M>
void receive_messages(salias::Subscriber<M>& subscriber, std::uint64_t count, const char* label) {
  PollContext context{.label = label};
  while (context.received < count) {
    // 批量 poll 最多 64 帧，减少单帧往返开销、提升吞吐
    const auto polled = subscriber.poll(64, print_message_callback, &context);
    if (polled == 0) {
      // 本轮无新消息，让出 CPU 避免空转（简化的 backoff 策略）
      std::this_thread::yield();
    }
  }
}

}  // namespace salias_example
