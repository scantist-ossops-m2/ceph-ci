// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_operation.h"
#include "common/Formatter.h"
#include "crimson/common/log.h"
#include "crimson/osd/osd_operations/client_request.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

void OSDOperationRegistry::do_stop()
{
  // we need to decouple visiting the registry from destructing
  // ops because of the auto-unlink feature of boost::intrusive.
  // the list shouldn't change while iterating due to constrains
  // on iterator's validity.
  constexpr auto historic_reg_index =
    static_cast<size_t>(OperationTypeCode::historic_client_request);
  auto& historic_registry = get_registry<historic_reg_index>();
  std::vector<ClientRequest::ICRef> to_ref_down;
  std::transform(std::begin(historic_registry), std::end(historic_registry),
		 std::back_inserter(to_ref_down),
		 [] (const Operation& op) {
		   return ClientRequest::ICRef{
		     static_cast<const ClientRequest*>(&op),
		     /* add_ref= */ false
		   };
		 });
  // to_ref_down is going off
}

size_t OSDOperationRegistry::dump_client_requests(ceph::Formatter* f) const
{
  const auto& client_registry =
    get_registry<static_cast<size_t>(ClientRequest::type)>();
  logger().warn("{} num_ops={}", __func__, std::size(client_registry));
  for (const auto& op : client_registry) {
    op.dump(f);
  }
  return std::size(client_registry);
}

size_t OSDOperationRegistry::dump_historic_client_requests(ceph::Formatter* f) const
{
  const auto& historic_client_registry =
    get_registry<static_cast<size_t>(OperationTypeCode::historic_client_request)>(); //ClientRequest::type)>();
  f->open_object_section("op_history");
  f->dump_int("size", historic_client_registry.size());
  // TODO: f->dump_int("duration", history_duration.load());
  // the intrusive list is configured to not store the size
  size_t ops_count = 0;
  {
    f->open_array_section("ops");
    for (const auto& op : historic_client_registry) {
      op.dump(f);
      ++ops_count;
    }
    f->close_section();
  }
  f->close_section();
  return ops_count;
}

OperationThrottler::OperationThrottler(ConfigProxy &conf)
  : scheduler(crimson::osd::scheduler::make_scheduler(conf))
{
  conf.add_observer(this);
  update_from_config(conf);
}

void OperationThrottler::wake()
{
  while ((!max_in_progress || in_progress < max_in_progress) &&
	 !scheduler->empty()) {
    auto item = scheduler->dequeue();
    item.wake.set_value();
    ++in_progress;
    --pending;
  }
}

void OperationThrottler::release_throttle()
{
  ceph_assert(in_progress > 0);
  --in_progress;
  wake();
}

seastar::future<> OperationThrottler::acquire_throttle(
  crimson::osd::scheduler::params_t params)
{
  crimson::osd::scheduler::item_t item{params, seastar::promise<>()};
  auto fut = item.wake.get_future();
  scheduler->enqueue(std::move(item));
  return fut;
}

void OperationThrottler::dump_detail(Formatter *f) const
{
  f->dump_unsigned("max_in_progress", max_in_progress);
  f->dump_unsigned("in_progress", in_progress);
  f->open_object_section("scheduler");
  {
    scheduler->dump(*f);
  }
  f->close_section();
}

void OperationThrottler::update_from_config(const ConfigProxy &conf)
{
  max_in_progress = conf.get_val<uint64_t>("crimson_osd_scheduler_concurrency");
  wake();
}

const char** OperationThrottler::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "crimson_osd_scheduler_concurrency",
    NULL
  };
  return KEYS;
}

void OperationThrottler::handle_conf_change(
  const ConfigProxy& conf,
  const std::set<std::string> &changed)
{
  update_from_config(conf);
}

}
