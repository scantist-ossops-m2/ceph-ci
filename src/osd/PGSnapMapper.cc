#include "PGSnapMapper.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "snap_mapper."
using result_t = Scrub::SnapMapReaderI::result_t;
using code_t = Scrub::SnapMapReaderI::result_t::code_t;

//---------------------------------------------------------------------------
bool PGSnapMapper::report_check_error(const hobject_t &hoid) const {
  derr << __func__ << " " << hoid << " mask_bits " << mask_bits
       << " match 0x" << std::hex << match << std::dec << " is false"
       << dendl;
  return false;
}

//---------------------------------------------------------------------------
/// Update bits in case of pg split or merge
void PGSnapMapper::update_bits(uint32_t new_split_bits)
{
  mask_bits   = new_split_bits;
  hash_prefix = (match & ~((uint32_t)(~0) << mask_bits));
  //ceph_assert(hash_prefix == match);
  hash_prefix_reversed = hobject_t::_reverse_bits(hash_prefix);
#if 0
  dout(1) << "GBH::SNAPMAP::" << std::hex << "::mask_bits=0x" << mask_bits
	  << ", hash_prefix=0x" << hash_prefix << ", shard=0x" << shard << dendl;
#endif
}

//---------------------------------------------------------------------------
tl::expected<std::set<snapid_t>, result_t>
PGSnapMapper::get_snaps(const hobject_t &hoid) const
{
  std::set<snapid_t> snaps;
  gsnap_ref->get_snaps_for_scrubber(shard, hoid, snaps);

  if (snaps.empty()) {
    dout(10) << __func__ << " " << hoid << " got.empty()" << dendl;
    return tl::unexpected(result_t{code_t::not_found, -ENOENT});
  }

  return snaps;
}
