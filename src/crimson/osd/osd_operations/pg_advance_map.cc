// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/future.hh>

#include "include/types.h"
#include "common/Formatter.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/osdmap_service.h"
#include "crimson/osd/shard_services.h"
#include "crimson/osd/osd_operations/pg_advance_map.h"
#include "crimson/osd/osd_operation_external_tracking.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

PGAdvanceMap::PGAdvanceMap(
  ShardServices &shard_services, Ref<PG> pg, epoch_t from, epoch_t to,
  PeeringCtx &&rctx, bool do_init)
  : shard_services(shard_services), pg(pg), from(from), to(to),
    rctx(std::move(rctx)), do_init(do_init)
  {
    logger().debug("{}: created", *this);
  }

PGAdvanceMap::~PGAdvanceMap() {}

void PGAdvanceMap::print(std::ostream &lhs) const
{
  lhs << "PGAdvanceMap("
      << "pg=" << pg->get_pgid()
      << " from=" << from
      << " to=" << to;
  if (do_init) {
    lhs << " do_init";
  }
  lhs << ")";
}

void PGAdvanceMap::dump_detail(Formatter *f) const
{
  f->open_object_section("PGAdvanceMap");
  f->dump_stream("pgid") << pg->get_pgid();
  f->dump_int("from", from);
  f->dump_int("to", to);
  f->dump_bool("do_init", do_init);
  f->close_section();
}

seastar::future<> PGAdvanceMap::start()
{
  using cached_map_t = OSDMapService::cached_map_t;

  logger().debug("{}: start", *this);

  IRef ref = this;
  return enter_stage<>(
    pg->peering_request_pg_pipeline.process
  ).then([this] {
    if (!do_init && to == pg->get_osdmap_epoch()) {
      /*
       * PGAdvanceMap is scheduled at pg creation
       * and when broadcasting new osdmaps to pgs.
       * The former's future is not chained and therfore
       * we are not able to serialize between the different
       * PGAdvanceMap callers. As a result the pg may already
       * get advanced (at it's creation) to the latest osdmap
       * epcoh. Therfore, we can safely ignore this event.
       */
      logger().debug("{}: pg was already advanced to {} at creation,"
                     " skipping", *this, pg->get_osdmap_epoch());
      return seastar::now();
    }
    if (from != pg->get_osdmap_epoch()) {
      /*
       * We are not scheduling PGAdvanceMap to pgs in 'creating' state.
       * Therfore, we may skip few osdmaps epochs until the pg
       * 'creating' state is erased. We need to pull back the
       * 'from' epoch to the latest pg osdmap epoch to avoid these gaps.
       * This case is only true for newly created pgs.
       * It is safe to pull back the 'from' epoch because:
       * 1) We handle each MOSDMap exclusive epoch once.
       * 2) The pg was not yet advanced on the range [from, osdmap_epoch].
       */
      logger().debug("{}: start pulling back from epoch to pg osdmap"
                     " {}->{}", *this, from, pg->get_osdmap_epoch());
      ceph_assert(std::cmp_greater(from, pg->get_osdmap_epoch()));
      from = pg->get_osdmap_epoch();
    }
    ceph_assert(std::cmp_less_equal(from, to));
    return seastar::do_for_each(
    boost::make_counting_iterator(from + 1),
    boost::make_counting_iterator(to + 1),
    [this](epoch_t next_epoch) {
    logger().debug("{}: start: getting map {}",
                   *this, next_epoch);
    return shard_services.get_map(next_epoch).then(
      [this] (cached_map_t&& next_map) {
        logger().debug("{}: advancing map to {}",
           *this, next_map->get_epoch());
        return pg->handle_advance_map(next_map, rctx);
      });
    }).then([this] {
    return pg->handle_activate_map(rctx).then([this] {
      logger().debug("{}: map activated", *this);
      return seastar::when_all_succeed(
        pg->get_need_up_thru()
        ? shard_services.send_alive(pg->get_same_interval_since())
        : seastar::now(),
        shard_services.dispatch_context(
          pg->get_collection_ref(),
        std::move(rctx))
      );
    });
  }).then_unpack([this] {
    logger().debug("{}: sending pg temp", *this);
    return shard_services.send_pg_temp();
  });
  }).then([this, ref=std::move(ref)] {
    logger().debug("{}: complete", *this);
  });
}

}
