#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <salias/extras/extras.hpp>
#include <string>

namespace {

pid_t wait_for_child(pid_t child, int& status) {
  pid_t result = -1;
  do {
    result = ::waitpid(child, &status, 0);
  } while (result < 0 && errno == EINTR);
  return result;
}

}  // namespace

int main() {
  using Keyed = salias::extras::LatestByKeyChannel<std::uint64_t, std::uint64_t, 4>;
  const std::string name = "install-extras-" + std::to_string(::getpid());
  auto owner = Keyed::create(name);
  if (!owner) {
    return 1;
  }
  auto subscriber = owner->subscriber();

  const pid_t child = ::fork();
  if (child == 0) {
    auto connected = Keyed::connect(name);
    if (!connected) {
      ::_exit(10);
    }
    auto publisher = connected->publisher();
    const auto offered = publisher.offer(7, 42);
    ::_exit(offered && *offered ? 0 : 11);
  }
  if (child < 0) {
    return 2;
  }

  int status = 0;
  if (wait_for_child(child, status) != child || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return 3;
  }
  auto received = subscriber.recv();
  if (!received || received->key != 7 || received->value != 42 || received->version != 1) {
    return 4;
  }

  salias::extras::Ringbuffer<int, 2> queue;
  if (queue.push(1) || !queue.push(2) || queue.try_pop() != 2) {
    return 5;
  }

  salias::extras::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> priority;
  auto invalid = priority.recv(salias::extras::PriorityWaitPolicy::Yield);
  if (invalid || invalid.error() != salias::Error::BadConfig) {
    return 6;
  }
  return 0;
}
