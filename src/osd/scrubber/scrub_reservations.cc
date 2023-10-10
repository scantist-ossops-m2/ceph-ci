// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_reservations.h"

#include "common/ceph_time.h"
#include "messages/MOSDScrubReserve.h"
#include "osd/OSD.h"
#include "osd/PG.h"
#include "osd/osd_types_fmt.h"

#include "pg_scrubber.h"

using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;

#define dout_context (m_osds->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this, __func__)

template <class T>
static ostream& _prefix(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

namespace Scrub {

ReplicaReservations::ReplicaReservations(ScrubMachineListener& scrbr)
    : m_scrubber{scrbr}
    , m_pg{m_scrubber.get_pg()}
    , m_whoami{m_pg->pg_whoami}
    , m_pgid{m_scrubber.get_spgid()}
    , m_osds{m_pg->get_pg_osd(ScrubberPasskey())}
{
  // the acting set is sorted by pg_shard_t. The reservations are to be issued
  // in this order, so that the OSDs will receive the requests in a consistent
  // order. This is done to reduce the chance of having two PGs that share some
  // of their acting-set OSDs, consistently interfering with each other's
  // reservation process.
  auto acting = m_pg->get_actingset();
  m_sorted_secondaries.reserve(acting.size());
  std::copy_if(
      acting.cbegin(), acting.cend(), std::back_inserter(m_sorted_secondaries),
      [whoami=m_whoami](const pg_shard_t& shard) { return shard != whoami; });

  m_next_to_request = m_sorted_secondaries.cbegin();
  // send out the 1'st request (unless we have no replicas)
  send_next_reservation_or_complete();
}

void ReplicaReservations::release_all()
{
  std::span<const pg_shard_t> replicas{
      m_sorted_secondaries.cbegin(), m_next_to_request};
  dout(10) << fmt::format("releasing {}", replicas) << dendl;
  epoch_t epoch = m_pg->get_osdmap_epoch();

  // send 'release' messages to all replicas we have managed to reserve
  for (const auto& peer : replicas) {
    auto m = make_message<MOSDScrubReserve>(
	spg_t{m_pgid.pgid, peer.shard}, epoch, MOSDScrubReserve::RELEASE,
	m_whoami);
    m_pg->send_cluster_message(peer.osd, m, epoch, false);
  }

  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

void ReplicaReservations::mark_failure(std::string_view msg_txt)
{
  dout(10) << fmt::format(
		  "failure ({}) while awaiting reply from {}",
		  msg_txt, get_last_sent().value_or(pg_shard_t{}))
	   << dendl;
  m_scrubber.flag_reservations_failure();
  // the Scrubber must release all resources and abort the scrubbing
  m_scrubber.clear_pgscrub_state();
}

void ReplicaReservations::discard_remote_reservations()
{
  dout(10) << "reset w/o issuing messages" << dendl;
  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

ReplicaReservations::~ReplicaReservations()
{
  release_all();
}

void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  // verify that the grant is from the peer we expected. If not?
  // for now - abort the OSD. \todo reconsider the reaction.
  if (!get_last_sent().has_value() || from != *get_last_sent()) {
    dout(1) << fmt::format(
		   "unexpected grant from {} (expected {})", from,
		   get_last_sent().value_or(pg_shard_t{}))
	    << dendl;
    ceph_assert(from == get_last_sent());
    return;
  }

  auto elapsed = clock::now() - m_last_request_sent_at;
  // \todo: was this response late?
  dout(10) << fmt::format(
		  "granted by {} ({} of {}) in {}ms",
		  from, active_requests_cnt(),
		  m_sorted_secondaries.size(),
		  duration_cast<milliseconds>(elapsed).count())
	   << dendl;
  send_next_reservation_or_complete();
}

void ReplicaReservations::send_next_reservation_or_complete()
{
  if (m_next_to_request == m_sorted_secondaries.cend()) {
    // granted by all replicas
    dout(10) << "remote reservation complete" << dendl;
    m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);

  } else {
    // send the next reservation request
    const auto peer = *m_next_to_request;
    const auto epoch = m_pg->get_osdmap_epoch();
    auto m = make_message<MOSDScrubReserve>(
	spg_t{m_pgid.pgid, peer.shard}, epoch, MOSDScrubReserve::REQUEST,
	m_whoami);
    m_pg->send_cluster_message(peer.osd, m, epoch, false);
    m_last_request_sent_at = clock::now();
    dout(10) << fmt::format(
		    "reserving {} ({} of {})", *m_next_to_request,
		    active_requests_cnt(), m_sorted_secondaries.size())
	     << dendl;
    m_next_to_request++;
  }
}

void ReplicaReservations::verify_rejections_source(
    OpRequestRef op,
    pg_shard_t from)
{
  // a convenient log message for the reservation process conclusion
  // (matches the one in send_next_reservation_or_complete())
  dout(10) << fmt::format(
		  "remote reservation failure. Rejected by {} ({})",
		  from, *op->get_req())
	   << dendl;

  // verify that the denial is from the peer we expected. If not?
  // we should treat it as though the *correct* peer has rejected the request,
  // but remember to release that peer, too.

  ceph_assert(get_last_sent().has_value());
  const auto expected = *get_last_sent();
  if (from != expected) {
    dout(1) << fmt::format(
		   "unexpected rejection from {} (expected {})",
		   from, expected)
	    << dendl;
  } else {
    // correct peer, wrong answer...
    m_next_to_request--;  // no need to release this one
  }
}

std::optional<pg_shard_t> ReplicaReservations::get_last_sent() const
{
  if (m_next_to_request == m_sorted_secondaries.cbegin()) {
    return std::nullopt;
  }
  return *(m_next_to_request - 1);
}

size_t ReplicaReservations::active_requests_cnt() const
{
  return m_next_to_request - m_sorted_secondaries.cbegin();
}

std::ostream& ReplicaReservations::gen_prefix(std::ostream& out, std::string fn)
    const
{
  return m_pg->gen_prefix(out)
	 << "scrubber::ReplicaReservations:" << fn << ": ";
}

}  // namespace Scrub