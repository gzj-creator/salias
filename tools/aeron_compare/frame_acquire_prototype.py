#!/usr/bin/env python3
"""Experimental FIFO frame publication; unchecked mode is UNSAFE for variable frames."""

from build_design_variants import replace_once


def transform(text, checked=False):
    # All reads/writes of shared-channel frame headers use atomic_ref in this variant.
    helpers = """
namespace frame_acquire_experiment {
inline std::uint64_t load(const ring::MagicRing& ring, std::uint64_t position) noexcept {
  auto bytes = ring.slice(position, frame::kHeaderSize);
  auto* word = reinterpret_cast<std::uint64_t*>(const_cast<std::byte*>(bytes.data()));
  return std::atomic_ref<std::uint64_t>(*word).load(std::memory_order_acquire);
}
inline void store(ring::MagicRing& ring, std::uint64_t position,
                  std::uint32_t length, std::uint32_t meta) noexcept {
  auto bytes = ring.slice_mut(position, frame::kHeaderSize);
  auto* word = reinterpret_cast<std::uint64_t*>(bytes.data());
  // Commit holes are still hidden: COMMITTED becomes visible only at the prefix frontier.
  std::atomic_ref<std::uint64_t>(*word).store(hybrid_detail::pack_header(
      length, meta & ~frame::FLAG_COMMITTED), std::memory_order_release);
}
}  // namespace frame_acquire_experiment

"""
    text = replace_once(text, "namespace consumer_registration {", helpers + "namespace consumer_registration {")
    text = text.replace("hybrid_detail::store_header_plain(", "frame_acquire_experiment::store(")
    text = text.replace("hybrid_detail::load_header_plain(", "frame_acquire_experiment::load(")
    text = replace_once(text, "        published_pos_ = end;", """        for (auto position = published_pos_; position < end;) {
          auto bytes = producer.ring.slice_mut(position, frame::kHeaderSize);
          auto* word = reinterpret_cast<std::uint64_t*>(bytes.data());
          const auto header = std::atomic_ref<std::uint64_t>(*word).load(std::memory_order_relaxed);
          const auto meta = static_cast<std::uint32_t>(header >> 32u);
          const auto length = static_cast<std::uint32_t>(header);
          std::atomic_ref<std::uint64_t>(*word).store(
              header | (static_cast<std::uint64_t>(frame::FLAG_COMMITTED) << 32u),
              std::memory_order_release);
          position += frame::frame_len(length, (meta & frame::FLAG_HAS_EXPIRY) != 0);
        }
        published_pos_ = end;""")
    if checked:
        # Header-first polling may reject early, but MUST still validate the visible prefix.
        # Stale payload bytes can impersonate a matching header after variable-length wrap.
        for indent, following in (("            ", "              auto* visible_pos =\n"),
                                  ("          ", "            auto* visible_pos = channel_")):
            anchor = indent + "if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {\n" + following
            gate = indent + """if constexpr (Ordering == Order::Fifo && !DropOldest) {
""" + indent + """  const auto header = frame_acquire_experiment::load(
""" + indent + """      channel_->producer_rings_[producer_id].ring, read_positions_[producer_id]);
""" + indent + """  const auto meta = static_cast<std::uint32_t>(header >> 32u);
""" + indent + """  if ((meta & frame::FLAG_COMMITTED) == 0 ||
""" + indent + """      frame::sequence_low_from_meta(meta) != frame::sequence_low(next_sequences_[producer_id])) {
""" + indent + """    continue;
""" + indent + """  }
""" + indent + """}
"""
            text = replace_once(text, anchor, gate + anchor)
        return text
    # UNSAFE diagnostic-only mode; adversarial stale-payload contract test rejects it.
    # Only reliable FIFO skips the cursor: latest/ordered keep original admission checks.
    text = replace_once(text, "    if (position >= visible_bound) {\n      return std::nullopt;\n    }", """    if constexpr (Ordering == Order::Fifo && !DropOldest) {
      const auto header = frame_acquire_experiment::load(producer.ring, position);
      const auto meta = static_cast<std::uint32_t>(header >> 32u);
      if ((meta & frame::FLAG_COMMITTED) == 0 ||
          frame::sequence_low_from_meta(meta) != frame::sequence_low(target_sequence)) {
        return std::nullopt;
      }
      visible_bound = position + producer.ring.capacity();
    } else if (position >= visible_bound) {
      return std::nullopt;
    }""")
    # Suppress only the cursor refreshes in receive entry points, not TTL/lease/registration.
    for indent, following in (("            ", "              auto* visible_pos =\n"),
                              ("          ", "            auto* visible_pos = channel_")):
        anchor = indent + "if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {\n" + following
        text = replace_once(text, anchor, indent +
            "if constexpr (!(Ordering == Order::Fifo && !DropOldest)) "
            "if (read_positions_[producer_id] >= cached_visible_pos_[producer_id]) {\n" + following)
    return text
