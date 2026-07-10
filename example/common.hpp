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

namespace salias_example {

struct Options {
  std::string name;
  std::uint64_t count = 10;
  std::size_t capacity = 1u << 20;
  std::string message;
  bool create = false;
};

struct ParseSpec {
  bool allow_create = false;
  bool allow_capacity = false;
  bool allow_message = false;
};

enum class ParseStatus { Ok, Help, Error };

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
  }
  return "Unknown";
}

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

inline bool require_value(int& index, int argc, char** argv, std::string_view option,
                          std::string_view& value, std::ostream& err) {
  if (index + 1 >= argc) {
    err << "missing value for " << option << '\n';
    return false;
  }
  value = argv[++index];
  return true;
}

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
      if (!require_value(i, argc, argv, arg, value, err) || !parse_unsigned(value, options.count) ||
          options.count == 0) {
        err << "invalid --count value\n";
        return ParseStatus::Error;
      }
    } else if (arg == "--capacity" && spec.allow_capacity) {
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
      return std::unexpected(last_error);
    }
    std::this_thread::yield();
  }
  return std::unexpected(last_error);
}

template <salias::Mode M>
salias::Result<bool> publish_text(salias::Publisher<M>& publisher, std::string_view text) {
  for (;;) {
    auto claimed = publisher.try_claim(text.size());
    if (claimed) {
      auto claim = std::move(claimed).value();
      std::memcpy(claim.payload().data(), text.data(), text.size());
      claim.commit();
      return true;
    }
    if (claimed.error() != salias::Error::BackPressured) {
      return std::unexpected(claimed.error());
    }
    std::this_thread::yield();
  }
}

struct PollContext {
  const char* label = "";
  std::uint64_t received = 0;
};

inline void print_message_callback(const salias::Message& message, void* user) noexcept {
  auto* context = static_cast<PollContext*>(user);
  ++context->received;
  std::cout << context->label << " received[" << context->received << "]: ";
  std::cout.write(reinterpret_cast<const char*>(message.payload.data()),
                  static_cast<std::streamsize>(message.payload.size()));
  std::cout << '\n';
  std::cout.flush();
}

template <salias::Mode M>
void receive_messages(salias::Subscriber<M>& subscriber, std::uint64_t count, const char* label) {
  PollContext context{.label = label};
  while (context.received < count) {
    const auto polled = subscriber.poll(64, print_message_callback, &context);
    if (polled == 0) {
      std::this_thread::yield();
    }
  }
}

}  // namespace salias_example
