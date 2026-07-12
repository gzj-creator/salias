#include <salias/config.hpp>

template <class Config>
concept HasFixedSize = requires(Config config) { config.fixed_size; };

template <class Config>
concept HasRecordSize = requires(Config config) { config.record_size; };

static_assert(!HasFixedSize<salias::Config>);
static_assert(!HasRecordSize<salias::Config>);

int main() {}
