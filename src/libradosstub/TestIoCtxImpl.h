// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_IO_CTX_IMPL_H
#define CEPH_TEST_IO_CTX_IMPL_H

#include <list>
#include <atomic>

#include <boost/function.hpp>

#include "include/rados/librados.hpp"
#include "include/Context.h"
#include "common/snap_types.h"

namespace librados {

class TestClassHandler;
class TestIoCtxImpl;
class TestRadosClient;

typedef boost::function<int(TestIoCtxImpl*,
			    const std::string&,
			    bufferlist *,
          uint64_t,
          const SnapContext &,
          uint64_t*)> ObjectOperationTestImpl;
typedef std::list<ObjectOperationTestImpl> ObjectOperations;

struct TestObjectOperationImpl {
public:
  void get();
  void put();

  ObjectOperations ops;
private:
  std::atomic<uint64_t> m_refcount = { 0 };
};

class TestIoCtxImpl {
public:
  typedef boost::function<int(TestIoCtxImpl *, const std::string &)> Operation;


  TestIoCtxImpl();
  explicit TestIoCtxImpl(TestRadosClient *client, int64_t m_pool_id,
                         const std::string& pool_name);

  TestRadosClient *get_rados_client() {
    return m_client;
  }

  void get();
  void put();

  inline int64_t get_pool_id() const {
    return m_pool_id;
  }

  virtual TestIoCtxImpl *clone() = 0;

  virtual uint64_t get_instance_id() const;
  virtual int64_t get_id();
  virtual uint64_t get_last_version();
  virtual std::string get_pool_name();

  inline void locator_set_key(const std::string& key) {
    m_oloc.key = key;
  }

  inline void set_namespace(const std::string& namespace_name) {
    m_oloc.nspace = namespace_name;
  }
  inline std::string get_namespace() const {
    return m_oloc.nspace;
  }

  snap_t get_snap_read() const {
    return m_snap_seq;
  }

  inline void set_snap_context(const SnapContext& snapc) {
    m_snapc = snapc;
  }
  const SnapContext &get_snap_context() const {
    return m_snapc;
  }

  virtual int aio_flush();
  virtual void aio_flush_async(AioCompletionImpl *c);
  virtual void aio_notify(const std::string& oid, AioCompletionImpl *c,
                          bufferlist& bl, uint64_t timeout_ms, bufferlist *pbl);
  virtual int aio_operate(const std::string& oid, TestObjectOperationImpl &ops,
                          AioCompletionImpl *c, SnapContext *snap_context,
                          int flags);
  virtual int aio_operate_read(const std::string& oid, TestObjectOperationImpl &ops,
                               AioCompletionImpl *c, int flags,
                               bufferlist *pbl, uint64_t snap_id,
                               uint64_t* objver);
  virtual int aio_append(const std::string& oid, AioCompletionImpl *c,
                         const bufferlist& bl, size_t len)  = 0;
  virtual int aio_remove(const std::string& oid, AioCompletionImpl *c,
                         int flags = 0) = 0;
  virtual int aio_watch(const std::string& o, AioCompletionImpl *c,
                        uint64_t *handle, librados::WatchCtx2 *ctx);
  virtual int aio_unwatch(uint64_t handle, AioCompletionImpl *c);
  virtual int aio_exec(const std::string& oid, AioCompletionImpl *c,
                       TestClassHandler *handler,
                       const char *cls, const char *method,
                       bufferlist& inbl, bufferlist *outbl);
  virtual int append(const std::string& oid, const bufferlist &bl,
                     const SnapContext &snapc) = 0;
  virtual int assert_exists(const std::string &oid, uint64_t snap_id) = 0;
  virtual int assert_version(const std::string &oid, uint64_t ver) = 0;

  virtual int create(const std::string& oid, bool exclusive,
                     const SnapContext &snapc) = 0;
  virtual int exec(const std::string& oid, TestClassHandler *handler,
                   const char *cls, const char *method,
                   bufferlist& inbl, bufferlist* outbl,
                   uint64_t snap_id, const SnapContext &snapc);
  virtual int list_snaps(const std::string& o, snap_set_t *out_snaps) = 0;
  virtual int list_watchers(const std::string& o,
                            std::list<obj_watch_t> *out_watchers);
  virtual int notify(const std::string& o, bufferlist& bl,
                     uint64_t timeout_ms, bufferlist *pbl);
  virtual void notify_ack(const std::string& o, uint64_t notify_id,
                          uint64_t handle, bufferlist& bl);
  virtual int omap_get_keys2(const std::string& oid,
                             const std::string& start_after,
                             uint64_t max_return,
                             std::set<std::string> *out_keys,
                             bool *pmore);
  virtual int omap_get_vals(const std::string& oid,
                            const std::string& start_after,
                            const std::string &filter_prefix,
                            uint64_t max_return,
                            std::map<std::string, bufferlist> *out_vals) = 0;
  virtual int omap_get_vals2(const std::string& oid,
                            const std::string& start_after,
                            const std::string &filter_prefix,
                            uint64_t max_return,
                            std::map<std::string, bufferlist> *out_vals,
                            bool *pmore) = 0;
  virtual int omap_get_vals_by_keys(const std::string& oid,
                                    const std::set<std::string>& keys,
                                    std::map<std::string, bufferlist> *vals) = 0;
  virtual int omap_rm_keys(const std::string& oid,
                           const std::set<std::string>& keys) = 0;
  virtual int omap_rm_range(const std::string& oid,
                            const string& key_begin,
                            const string& key_end) = 0;
  virtual int omap_clear(const std::string& oid) = 0;
  virtual int omap_set(const std::string& oid,
                       const std::map<std::string, bufferlist> &map) = 0;
  virtual int omap_get_header(const std::string& oid,
                              bufferlist *bl) = 0;
  virtual int omap_set_header(const std::string& oid,
                              const bufferlist& bl) = 0;
  virtual int operate(const std::string& oid, TestObjectOperationImpl &ops, int flags);
  virtual int operate_read(const std::string& oid, TestObjectOperationImpl &ops,
                           bufferlist *pbl, int flags);
  virtual int read(const std::string& oid, size_t len, uint64_t off,
                   bufferlist *bl, uint64_t snap_id, uint64_t* objver) = 0;
  virtual int remove(const std::string& oid, const SnapContext &snapc) = 0;
  virtual int selfmanaged_snap_create(uint64_t *snapid) = 0;
  virtual void aio_selfmanaged_snap_create(uint64_t *snapid,
                                           AioCompletionImpl *c);
  virtual int selfmanaged_snap_remove(uint64_t snapid) = 0;
  virtual void aio_selfmanaged_snap_remove(uint64_t snapid,
                                           AioCompletionImpl *c);
  virtual int selfmanaged_snap_rollback(const std::string& oid,
                                        uint64_t snapid) = 0;
  virtual int selfmanaged_snap_set_write_ctx(snap_t seq,
                                             std::vector<snap_t>& snaps);
  virtual int set_alloc_hint(const std::string& oid,
                             uint64_t expected_object_size,
                             uint64_t expected_write_size,
                             uint32_t flags,
                             const SnapContext &snapc);
  virtual void set_snap_read(snap_t seq);
  virtual int sparse_read(const std::string& oid, uint64_t off, uint64_t len,
                          std::map<uint64_t,uint64_t> *m,
                          bufferlist *data_bl, uint64_t snap_id) = 0;
  virtual int stat(const std::string& oid, uint64_t *psize, time_t *pmtime);
  virtual int stat2(const std::string& oid, uint64_t *psize, struct timespec *pts) = 0;
  virtual int mtime2(const string& oid, const struct timespec& ts,
                     const SnapContext &snapc) = 0;
  virtual int truncate(const std::string& oid, uint64_t size,
                       const SnapContext &snapc) = 0;
  virtual int tmap_update(const std::string& oid, bufferlist& cmdbl);
  virtual int unwatch(uint64_t handle);
  virtual int watch(const std::string& o, uint64_t *handle,
                    librados::WatchCtx *ctx, librados::WatchCtx2 *ctx2);
  virtual int write(const std::string& oid, bufferlist& bl, size_t len,
                    uint64_t off, const SnapContext &snapc) = 0;
  virtual int write_full(const std::string& oid, bufferlist& bl,
                         const SnapContext &snapc) = 0;
  virtual int writesame(const std::string& oid, bufferlist& bl, size_t len,
                        uint64_t off, const SnapContext &snapc) = 0;
  virtual int cmpext(const std::string& oid, uint64_t off, bufferlist& cmp_bl,
                     uint64_t snap_id) = 0;
  virtual int cmpxattr_str(const std::string& oid, const char *name,
                           uint8_t op, const bufferlist& bl) = 0;
  virtual int cmpxattr(const std::string& oid, const char *name,
                       uint8_t op, uint64_t v) = 0;
  virtual int getxattr(const string& oid, const char *name, bufferlist *pbl);
  virtual int xattr_get(const std::string& oid,
                        std::map<std::string, bufferlist>* attrset) = 0;
  virtual int setxattr(const std::string& oid, const char *name,
                       bufferlist& bl) = 0;
  virtual int rmxattr(const std::string& oid, const char *name) = 0;
  virtual int zero(const std::string& oid, uint64_t off, uint64_t len,
                   const SnapContext &snapc) = 0;

  virtual int get_current_ver(const std::string& oid, uint64_t *ver) = 0;

  int execute_operation(const std::string& oid,
                        const Operation &operation);

protected:
  TestIoCtxImpl(const TestIoCtxImpl& rhs);
  virtual ~TestIoCtxImpl();

  int execute_aio_operations(const std::string& oid,
                             TestObjectOperationImpl *ops,
                             bufferlist *pbl, uint64_t,
                             const SnapContext &snapc,
                             int flags,
                             uint64_t* objver);

private:
  struct C_AioNotify : public Context {
    TestIoCtxImpl *io_ctx;
    AioCompletionImpl *aio_comp;
    C_AioNotify(TestIoCtxImpl *_io_ctx, AioCompletionImpl *_aio_comp)
      : io_ctx(_io_ctx), aio_comp(_aio_comp) {
    }
    void finish(int r) override {
      io_ctx->handle_aio_notify_complete(aio_comp, r);
    }
  };

  struct Locator {
    std::string key;
    std::string nspace;
  };

  TestRadosClient *m_client;
  int64_t m_pool_id = 0;
  std::string m_pool_name;
  Locator m_oloc;

  snap_t m_snap_seq = 0;
  SnapContext m_snapc;
  std::atomic<uint64_t> m_refcount = { 0 };
  std::atomic<uint64_t> m_pending_ops = { 0 };

  void handle_aio_notify_complete(AioCompletionImpl *aio_comp, int r);
};

} // namespace librados

#endif // CEPH_TEST_IO_CTX_IMPL_H
