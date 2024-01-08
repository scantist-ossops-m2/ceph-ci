// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_MULTI_UPLOAD_PART_INFO_H
#define CEPH_RGW_MULTI_UPLOAD_PART_INFO_H

struct RGWUploadPartInfo {
  uint32_t num;
  uint64_t size;
  uint64_t accounted_size{0};
  std::string etag;
  ceph::real_time modified;
  RGWObjManifest manifest;
  RGWCompressionInfo cs_info;

  // Previous part obj prefixes. Recorded here for later cleanup.
  std::set<std::string> past_prefixes;

  RGWUploadPartInfo() : num(0), size(0) {}

  void encode(bufferlist& bl) const {
    ENCODE_START(5, 2, bl);
    encode(num, bl);
    encode(size, bl);
    encode(etag, bl);
    encode(modified, bl);
    encode(manifest, bl);
    encode(cs_info, bl);
    encode(accounted_size, bl);
    encode(past_prefixes, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(5, 2, 2, bl);
    decode(num, bl);
    decode(size, bl);
    decode(etag, bl);
    decode(modified, bl);
    if (struct_v >= 3)
      decode(manifest, bl);
    if (struct_v >= 4) {
      decode(cs_info, bl);
      decode(accounted_size, bl);
    } else {
      accounted_size = size;
    }
    if (struct_v >= 5) {
      decode(past_prefixes, bl);
    }
    DECODE_FINISH(bl);
  }
  void dump(Formatter *f) const;
  static void generate_test_instances(std::list<RGWUploadPartInfo*>& o);
};
WRITE_CLASS_ENCODER(RGWUploadPartInfo)

#endif
