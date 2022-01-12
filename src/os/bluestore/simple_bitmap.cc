#include "simple_bitmap.h"

#if 1
#include "include/ceph_assert.h"
#include "bluestore_types.h"
#include "common/debug.h"

//#include "osd_types.h"
#define dout_context cct
#define dout_subsys  ceph_subsys_bluestore

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore
#undef dout_prefix
#define dout_prefix *_dout << "SBMAP::" << this << " "
#endif

static struct extent_t null_extent = {0, 0};

//----------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& out, const SimpleBitmap& bmap)
{
  for (unsigned i = 0; i < bmap.m_word_count; i++) {      
    if(bmap.m_arr[i] == 0) {
      continue;
    }

    out << "[" << i << "]" << "0x" << std::setfill('0') << std::setw(2) << std::right << std::hex;
	  
    if (SimpleBitmap::BYTES_IN_WORD > 1) {
      out << bmap.m_arr[i] << std::endl;
    } else {
      // hex printout doesn't work for uint8_t, must convert to higher integer
      out << (unsigned) bmap.m_arr[i] << std::endl;
    }
  }
  return out;
}

//----------------------------------------------------------------------------
//throw bad_alloc
SimpleBitmap::SimpleBitmap(CephContext *_cct, const std::string &_path, uint64_t num_bits) :cct(_cct), path(_path)
{
  m_num_bits   = num_bits;
  m_word_count = bits_to_words(num_bits);
  if (num_bits % BITS_IN_WORD) {
    m_word_count++;
  }
  m_arr = new bitmap_word_t [m_word_count];
  clear_all();
  dout(20) << "m_word_count  = " << m_word_count  << dendl;
  dout(20) << "m_num_bits    = " << m_num_bits    << dendl;
  dout(20) << "BYTES_IN_WORD = " << BYTES_IN_WORD << dendl;
  dout(20) << "BITS_IN_WORD  = " << BITS_IN_WORD  << dendl;
  dout(20) << "FULL_MASK     = " << std::hex << (uint64_t)FULL_MASK << std::dec << dendl;
}

//----------------------------------------------------------------------------
SimpleBitmap::~SimpleBitmap()
{
  delete [] m_arr;
}

//----------------------------------------------------------------------------
bool SimpleBitmap::set(uint64_t offset, uint64_t length)
{
  dout(20) << __func__ <<" [" << std::hex << offset << ", " << length << "]" << dendl;
  if (offset + length > m_num_bits) {
    derr << "offset + length = " << offset + length << " exceeds map size = " << m_num_bits << dendl;
    ceph_assert(offset + length <= m_num_bits);
    return false;
  }
    
  // find the first word index
  uint64_t index = offset_to_index(offset);
  // handle the first word which might be incomplete
  uint64_t first_bit_set = offset % BITS_IN_WORD;

  // special case optimization
  if (length == 1) {
    bitmap_word_t set_mask = 1ULL << first_bit_set;
    m_arr[index] |= set_mask;
    return true;
  }

  if (first_bit_set != 0) {
    bitmap_word_t set_mask      = FULL_MASK << first_bit_set;
    uint64_t      first_bit_clr = first_bit_set + length;
    if (first_bit_clr < BITS_IN_WORD) { 
      uint64_t      clr_bits = BITS_IN_WORD - first_bit_clr;
      bitmap_word_t clr_mask = FULL_MASK >> clr_bits;
      set_mask     &= clr_mask;
      m_arr[index] |= set_mask;
      return true;
    } else {
      // set all bits in this word starting from first_bit_set
      m_arr[index] |= set_mask;
      index ++;
      length -= (BITS_IN_WORD - first_bit_set);
    }
  }

  // set a range of full words
  uint64_t full_words_count = bits_to_words(length);
  uint64_t end_range        = index + full_words_count;
  for ( ; index < end_range; index++ ) {
    m_arr[index] = FULL_MASK;
  }
  length -= words_to_bits(full_words_count);

  // set bits in the last word
  if (length) {
    bitmap_word_t set_mask = ~(FULL_MASK << length);
    m_arr[index] |= set_mask;
  }

  return true;
}

//----------------------------------------------------------------------------
bool SimpleBitmap::clr(uint64_t offset, uint64_t length)
{
  dout(20) << __func__ <<" [" << std::hex << offset << ", " << length << "]" << dendl;
  if (offset + length > m_num_bits) {
    derr << "offset + length = " << offset + length <<
      " exceeds map size = " << m_num_bits << dendl;
    ceph_assert(offset + length <= m_num_bits);
    return false;
  }
    
  // find the first word index
  uint64_t index = offset_to_index(offset);
  // handle the first word which might be incomplete
  uint64_t first_bit_clr = offset % BITS_IN_WORD;

  // special case optimization
  if (length == 1) {
    bitmap_word_t set_mask = 1ULL << first_bit_clr;
    bitmap_word_t clr_mask = ~set_mask;
    m_arr[index] &= clr_mask;
    return true;
  }

  if (first_bit_clr != 0) {
    uint64_t      clr_bits      = BITS_IN_WORD - first_bit_clr;
    bitmap_word_t clr_mask      = FULL_MASK >> clr_bits;
    uint64_t      first_bit_set = first_bit_clr + length;
    if (first_bit_set < BITS_IN_WORD) { 
      uint64_t      set_mask = FULL_MASK << first_bit_set;
      clr_mask     |= set_mask;
      m_arr[index] &= clr_mask;
      return true;
    } else {
      // clear all bits in this word starting from first_bit_clr
      m_arr[index] &= clr_mask;
      index ++;
      length -= (BITS_IN_WORD - first_bit_clr);
    }
  }

  // set a range of full words
  uint64_t full_words_count = bits_to_words(length);
  uint64_t end_range        = index + full_words_count;
  for ( ; index < end_range; index++ ) {
    m_arr[index] = 0;
  }
  length -= words_to_bits(full_words_count);

  // set bits in the last word
  if (length) {
    bitmap_word_t clr_mask = (FULL_MASK << length);
    m_arr[index] &= clr_mask;
  }

  return true;
}

//----------------------------------------------------------------------------
void SimpleBitmap::start_itration_for_set_extents()
{
  // set the iterator on the first word not fully clear
  for (m_word_idx_set = 0; m_word_idx_set < m_word_count; m_word_idx_set++) {
    if (m_arr[m_word_idx_set]) {
      m_itr_word_set = m_arr[m_word_idx_set];
      break;
    }
  }
}
  
//----------------------------------------------------------------------------
extent_t SimpleBitmap::get_next_set_extent()
{
  dout(20) << "m_word_idx_set=" << m_word_idx_set << " m_itr_word_set=" << m_itr_word_set  << dendl;
  if (m_word_idx_set >= m_word_count ) {      
    dout(10) << "Reached the end of the bitmap" << dendl;
    return null_extent;
  }

  ceph_assert(m_itr_word_set);
  int           ffs = __builtin_ffsll(m_itr_word_set);
  extent_t      ext;
  ext.offset = words_to_bits(m_word_idx_set) + (ffs - 1);

  // set all bits from current to LSB
  uint64_t      set_bits = BITS_IN_WORD - ffs;
  bitmap_word_t set_mask = FULL_MASK >> set_bits;    
  m_itr_word_set |= set_mask;

  // skipped past fully set words
  if (m_itr_word_set == FULL_MASK) {
    while ( (++m_word_idx_set < m_word_count) && (m_arr[m_word_idx_set] == FULL_MASK) );

    if (m_word_idx_set < m_word_count) {
      m_itr_word_set = m_arr[m_word_idx_set];
    } else {
      // bitmap is set from ext.offset until the last bit
      ext.length = (m_num_bits - ext.offset);
      ceph_assert(m_num_bits % BITS_IN_WORD == 0);
      return ext;
    }
  }

  ceph_assert(m_itr_word_set != FULL_MASK);
  // reverse bits to allow ffs to be used
  m_itr_word_set = ~m_itr_word_set;

  // find the first clear bit (after reversing)
  int      ffz     = __builtin_ffsll(m_itr_word_set);
  uint64_t zoffset = words_to_bits(m_word_idx_set) + (ffz - 1);
  ext.length = (zoffset - ext.offset);
    
  // reverse bit back to normal
  m_itr_word_set = ~m_itr_word_set;
    
  // clear all bits before current position
  m_itr_word_set >>= ffz;
  m_itr_word_set <<= ffz;

  // skip past all clear words
  if ( (m_itr_word_set == 0) || (ffz == BITS_IN_WORD) ) {
    while ( (++m_word_idx_set < m_word_count) && (m_arr[m_word_idx_set] == 0) );
    if (m_word_idx_set < m_word_count) {
      m_itr_word_set = m_arr[m_word_idx_set];
    }
  }
  return ext;
}

//----------------------------------------------------------------------------
void SimpleBitmap::start_itration_for_clr_extents()
{
  // set the iterator on the first word not fully set
  for (m_word_idx_clr = 0; m_word_idx_clr < m_word_count; m_word_idx_clr++) {
    if (m_arr[m_word_idx_clr] != FULL_MASK) {
      m_itr_word_clr = m_arr[m_word_idx_clr];
      break;
    }
  }
}

//----------------------------------------------------------------------------
extent_t SimpleBitmap::get_next_clr_extent()
{
  dout(20) << "1)m_word_idx_clr=" << m_word_idx_clr << " m_itr_word_clr=" << std::hex << m_itr_word_clr << dendl;
  if (m_word_idx_clr >= m_word_count ) {
    dout(10) << "Reached the end of the bitmap" << dendl;
    return null_extent;
  }

  // reverse bits to allow ffs to be used
  m_itr_word_clr = ~m_itr_word_clr;
  ceph_assert(m_itr_word_clr);
  int      ffz = __builtin_ffsll(m_itr_word_clr);
  extent_t ext;
  ext.offset = words_to_bits(m_word_idx_clr) + (ffz - 1);

  // reverse bit back to normal
  m_itr_word_clr = ~m_itr_word_clr;
    
  // clear all bits from current position to LSB
  m_itr_word_clr >>= ffz;
  m_itr_word_clr <<= ffz;

  // skip past all clear words
  if ( (m_itr_word_clr == 0) || (ffz == BITS_IN_WORD) ) {
    while ( (++m_word_idx_clr < m_word_count) && (m_arr[m_word_idx_clr] == 0) );
    
    if (m_word_idx_clr < m_word_count) {
      m_itr_word_clr = m_arr[m_word_idx_clr];
    } else {
      dout(10) << "bitmap is free from ext.offset until the last bit" << dendl;
      dout(10) << "m_num_bits=" << m_num_bits << " ext.offset=" << ext.offset << dendl;
      ext.length = (m_num_bits - ext.offset);
      ceph_assert(m_num_bits % BITS_IN_WORD == 0);
      return ext;
    }
  }

  int           ffs     = __builtin_ffsll(m_itr_word_clr);
  uint64_t      soffset = words_to_bits(m_word_idx_clr) + (ffs - 1);
  ext.length = (soffset - ext.offset);	
  uint64_t      set_bits = BITS_IN_WORD - ffs;
  bitmap_word_t set_mask = FULL_MASK >> set_bits;
      
  m_itr_word_clr |= set_mask;
  if (m_itr_word_clr == FULL_MASK) {
    // skipped past fully set words
    while ( (++m_word_idx_clr < m_word_count) && (m_arr[m_word_idx_clr] == FULL_MASK) );
    
    if (m_word_idx_clr < m_word_count) {
      m_itr_word_clr = m_arr[m_word_idx_clr];
    }else {
      dout(10) << "We reached the end of the bitmap" << dendl;
    }
  }

  return ext;
}

  
