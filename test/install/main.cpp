#include <salias/salias.hpp>

int main() {
  salias::Config config;
  config.name = "install-consumer";
  return config.capacity == 0 ? 1 : 0;
}
