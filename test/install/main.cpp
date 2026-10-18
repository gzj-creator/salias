#include <salias/salias.hpp>

int main() {
  salias::Config config;
  config.name = "install-consumer";
  salias::PrioritySubscriber<salias::Mode::FifoMpsc, salias::Mode::FifoMpsc> reader;
  for (const auto policy : {salias::PriorityWaitPolicy::Yield, salias::PriorityWaitPolicy::Spin,
                            salias::PriorityWaitPolicy::Adaptive}) {
    auto result = reader.recv(policy);
    if (result || result.error() != salias::Error::BadConfig) {
      return 2;
    }
  }
  return config.capacity == 0 ? 1 : 0;
}
