#pragma once

#include "rgw_common.h"
#include "rgw_aio.h"

namespace rgw { namespace cache {

struct Partition {
    std::string name;
    std::string type;
    std::string location;
    uint64_t size;
};

class CacheDriver {
  /*struct CacheAioRequest {*/
    /* This will be invoked from rgw_aio.cc, to asynchronously read from / write to the cache backend. We will also need to define ops like the following in rgw_aio.h
    Aio::OpFunc Aio::cache_read_op(const DoutPrefixProvider* dpp, optional_yield y, off_t ofs, uint64_t len)
    Aio::OpFunc Aio::cache_write_op(const DoutPrefixProvider* dpp, optional_yield y, bufferlist& bl, uint64_t len)*/

    /*void cache_aio_read_abstract(const DoutPrefixProvider* dpp, boost::asio::io_context& context, yield_context yield, off_t ofs, uint64_t len, rgw::Aio* aio, rgw::AioResult r);
    void cache_aio_write_abstract(const DoutPrefixProvider* dpp, boost::asio::io_context& context, yield_context yield, bufferlist& bl, uint64_t len, rgw::Aio* aio, rgw::AioResult r);
  };*/

  /* For Write-back cache*/
  /*struct DirtyObjectsState {
    std::string key;
    ceph::real _time creation_time;
    ceph::real lifetime; // time after which it will be written out to the store
			       
    std::queue<DirtyObjectState> dirty_obs_queue;
    void write_to_store();
  };*/

  public:
    CacheDriver() {}
    virtual ~CacheDriver() = default;

    virtual int initialize(CephContext* cct, const DoutPrefixProvider* dpp) = 0;
    virtual int put(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int get(const DoutPrefixProvider* dpp, const std::string& key, off_t offset, uint64_t len, bufferlist& bl, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int del(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y) = 0;
    virtual rgw::AioResultList get_async (const DoutPrefixProvider* dpp, optional_yield y, rgw::Aio* aio, const std::string& key, off_t ofs, uint64_t len, uint64_t cost, uint64_t id) = 0;
    virtual int put_async(const DoutPrefixProvider* dpp, const std::string& key, bufferlist& bl, uint64_t len, rgw::sal::Attrs& attrs) = 0;
    virtual int append_data(const DoutPrefixProvider* dpp, const::std::string& key, bufferlist& bl_data, optional_yield y) = 0;
    virtual int delete_data(const DoutPrefixProvider* dpp, const::std::string& key, optional_yield y) = 0;
    virtual int get_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int set_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int update_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& attrs, optional_yield y) = 0;
    virtual int delete_attrs(const DoutPrefixProvider* dpp, const std::string& key, rgw::sal::Attrs& del_attrs, optional_yield y) = 0;
    virtual std::string get_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, optional_yield y) = 0;
    virtual int set_attr(const DoutPrefixProvider* dpp, const std::string& key, const std::string& attr_name, const std::string& attr_val, optional_yield y) = 0;

    /* Partition */
    virtual Partition get_current_partition_info(const DoutPrefixProvider* dpp) = 0;
    virtual uint64_t get_free_space(const DoutPrefixProvider* dpp) = 0;
};

} } // namespace rgw::cache

