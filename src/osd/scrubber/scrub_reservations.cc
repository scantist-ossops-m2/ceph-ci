// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_reservations.h"

#include <span>

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
#define dout_prefix _prefix_fn(_dout, this, __func__)
template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

namespace Scrub {

ReplicaReservations::ReplicaReservations(
    ScrubMachineListener& scrbr,
    PerfCounters& pc)
    : m_scrubber{scrbr}
    , m_pg{m_scrubber.get_pg()}
    , m_pgid{m_scrubber.get_spgid().pgid}
    , m_osds{m_pg->get_pg_osd(ScrubberPasskey())}
    , m_perf_set{pc}
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
      [whoami = m_pg->pg_whoami](const pg_shard_t& shard) {
	return shard != whoami;
      });
  m_perf_set.set(scrbcnt_resrv_replicas_num, m_sorted_secondaries.size());

  m_next_to_request = m_sorted_secondaries.cbegin();
  if (m_scrubber.is_high_priority()) {
    // for high-priority scrubs (i.e. - user-initiated), no reservations are
    // needed. Note: not perf-counted as either success or failure.
    dout(10) << "high-priority scrub - no reservations needed" << dendl;
    m_perf_set.inc(scrbcnt_resrv_skipped);
  } else {
    m_process_started_at = ScrubClock::now();

    // send out the 1'st request (unless we have no replicas)
    send_next_reservation_or_complete();
    m_slow_response_warn_timeout =
	m_scrubber.get_pg_cct()->_conf.get_val<milliseconds>(
	    "osd_scrub_slow_reservation_response");
  }
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
	spg_t{m_pgid, peer.shard}, epoch, MOSDScrubReserve::RELEASE,
	m_pg->pg_whoami);
    m_pg->send_cluster_message(peer.osd, m, epoch, false);
  }

  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

void ReplicaReservations::discard_remote_reservations()
{
  dout(10) << "reset w/o issuing messages" << dendl;
  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.cbegin();
}

void ReplicaReservations::log_success_and_duration()
{
  auto logged_duration = ScrubClock::now() - m_process_started_at.value();
  m_perf_set.tinc(scrbcnt_resrv_successful_elapsed, logged_duration);
  m_perf_set.inc(scrbcnt_resrv_success);
  m_osds->logger->hinc(
      l_osd_scrub_reservation_dur_hist, m_sorted_secondaries.size(),
      logged_duration.count());
  m_process_started_at.reset();
}

void ReplicaReservations::log_failure_and_duration(int failure_cause_counter)
{
  if (!m_process_started_at.has_value()) {
    // outcome (success/failure) already logged
    return;
  }
  auto logged_duration = ScrubClock::now() - m_process_started_at.value();
  m_perf_set.tinc(scrbcnt_resrv_failed_elapsed, logged_duration);
  m_process_started_at.reset();
  // note: not counted into l_osd_scrub_reservation_dur_hist
  m_perf_set.inc(failure_cause_counter);
}

ReplicaReservations::~ReplicaReservations()
{
  release_all();
  log_failure_and_duration(scrbcnt_resrv_aborted);
}

// the specific check for 'is this a relevant response' is expected to be
// improved in the future, to include a unique counter value (instead of the
// epoch, which is not guaranteed to change from one reservation attempt to
// another).
tl::expected<bool, std::string> ReplicaReservations::is_response_relevant(
    epoch_t msg_epoch,
    pg_shard_t from) const
{
  if (msg_epoch != m_last_request_sent_epoch) {
    return tl::unexpected(fmt::format(
	"stale reservation response from {} (response e:{} vs. expected {})",
	from, msg_epoch, m_last_request_sent_epoch));
  }
  return true;
}

tl::expected<bool, std::string> ReplicaReservations::is_msg_source_correct(
    epoch_t msg_epoch,
    pg_shard_t from) const
{
  const auto exp_source = get_last_sent();
  if (!exp_source || from != *exp_source) {
    return tl::unexpected(fmt::format(
	"unexpected response from {} (with e:{}) (expected {})", from,
	msg_epoch, exp_source.value_or(pg_shard_t{})));
  }
  return true;
}

bool ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  // is this a stale response to a previous request (e.g. one that
  // timed-out)? if so - ignore it (but do log an error, to cause tests
  // to fail)
  const auto msg_epoch = op->get_req<MOSDScrubReserve>()->map_epoch;
  const auto epoch_verified = is_response_relevant(msg_epoch, from);
  if (!epoch_verified) {
    m_osds->clog->warn() << epoch_verified.error();
    return false;
  }

  // verify that the grant is from the peer we expected. If not?
  // for now - abort the OSD. \todo reconsider the reaction.
  const auto peer_verified = is_msg_source_correct(msg_epoch, from);
  if (!peer_verified) {
    m_osds->clog->error() << peer_verified.error();
    ceph_abort_msg(peer_verified.error());
    return false;
  }

  auto elapsed = ScrubClock::now() - m_last_request_sent_at;

  // log a warning if the response was slow to arrive
  if ((m_slow_response_warn_timeout > 0ms) &&
      (elapsed > m_slow_response_warn_timeout)) {
    m_osds->clog->warn() << fmt::format(
		   "slow reservation response from {} ({}ms)", from,
		   duration_cast<milliseconds>(elapsed).count());
    // prevent additional warnings
    m_slow_response_warn_timeout = 0ms;
  }
  dout(10) << fmt::format(
		  "e:{} granted by {} ({} of {}) in {}ms", msg_epoch, from,
		  active_requests_cnt(), m_sorted_secondaries.size(),
		  duration_cast<milliseconds>(elapsed).count())
	   << dendl;
  return send_next_reservation_or_complete();
}

bool ReplicaReservations::send_next_reservation_or_complete()
{
  if (m_next_to_request == m_sorted_secondaries.cend()) {
    // granted by all replicas
    dout(10) << "remote reservation complete" << dendl;
    log_success_and_duration();
    return true;  // done
  }

  // send the next reservation request
  const auto peer = *m_next_to_request;
  m_last_request_sent_epoch = m_pg->get_osdmap_epoch();
  auto m = make_message<MOSDScrubReserve>(
      spg_t{m_pgid, peer.shard}, m_last_request_sent_epoch,
      MOSDScrubReserve::REQUEST, m_pg->pg_whoami);
  m_pg->send_cluster_message(peer.osd, m, m_last_request_sent_epoch, false);
  m_last_request_sent_at = ScrubClock::now();
  dout(10) << fmt::format(
		  "reserving {} (the {} of {} replicas) e:{}",
		  *m_next_to_request, active_requests_cnt() + 1,
		  m_sorted_secondaries.size(), m_last_request_sent_epoch)
	   << dendl;
  m_next_to_request++;
  return false;
}

bool  ReplicaReservations::handle_rejection(
    OpRequestRef op,
    pg_shard_t from)
{
  // a convenient log message for the reservation process conclusion
  // (matches the one in send_next_reservation_or_complete())
  dout(10) << fmt::format(
		  "remote reservation failure. Rejected by {} ({})", from,
		  *op->get_req())
	   << dendl;

  // is this a stale response to a previous request (e.g. one that
  // timed-out)? if so - ignore it (but do log an error, to cause tests
  // to fail)
  const auto msg_epoch = op->get_req<MOSDScrubReserve>()->map_epoch;
  const auto epoch_verified = is_response_relevant(msg_epoch, from);
  if (!epoch_verified) {
    m_osds->clog->warn() << epoch_verified.error();
    return false;
  }

  log_failure_and_duration(scrbcnt_resrv_rejected);
  ceph_assert(get_last_sent().has_value());
  // verify that the denial is from the peer we expected. If not?
  // we should treat it as though the *correct* peer has rejected the request,
  // but remember to release that peer, too.
  const auto peer_verified = is_msg_source_correct(msg_epoch, from);
  if (!peer_verified) {
    m_osds->clog->warn() << peer_verified.error();
  } else {
    // correct peer, wrong answer...
    m_next_to_request--;  // no need to release this one
  }
  return true;
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

std::ostream& ReplicaReservations::gen_prefix(
    std::ostream& out,
    std::string fn) const
{
  return m_pg->gen_prefix(out)
	 << fmt::format("scrubber::ReplicaReservations:{}: ", fn);
}

} // namespace Scrub
