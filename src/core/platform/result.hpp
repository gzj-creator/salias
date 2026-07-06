#pragma once

#include <utility>
#include <variant>

namespace salias::platform {

template <class T, class E>
class Result {
 public:
  static Result success(T value) { return Result(std::in_place_index<0>, std::move(value)); }
  static Result failure(E error) { return Result(std::in_place_index<1>, error); }

  bool has_value() const noexcept { return storage_.index() == 0; }
  explicit operator bool() const noexcept { return has_value(); }

  T& value() & { return std::get<0>(storage_); }
  const T& value() const& { return std::get<0>(storage_); }
  T&& value() && { return std::move(std::get<0>(storage_)); }

  E error() const { return std::get<1>(storage_); }

 private:
  template <std::size_t Index, class... Args>
  explicit Result(std::in_place_index_t<Index> index, Args&&... args)
      : storage_(index, std::forward<Args>(args)...) {}

  std::variant<T, E> storage_;
};

}  // namespace salias::platform
