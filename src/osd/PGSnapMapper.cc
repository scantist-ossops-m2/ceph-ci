#include "PGSnapMapper.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "snap_mapper."

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
  //dout(1) << "GBH::SNAPMAP::" << std::hex << "::mask_bits=0x" << mask_bits << ", hash_prefix=0x" << hash_prefix << ", shard=0x" << shard << dendl;
}
