// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <cassert>
#include <chrono>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "osd_scrub_sched.h"

namespace Scrub {

/**
 * Reserving/freeing scrub resources at the replicas.
 *
 * When constructed - sends reservation requests to the acting_set OSDs, one
 * by one.
 * Once a replica's OSD replies with a 'grant'ed reservation, we send a
 * reservation request to the next replica.
 * A rejection triggers a "couldn't acquire the replicas' scrub resources"
 * event. All granted reservations are released.
 *
 * Reserved replicas should be released at the end of the scrub session. The
 * one exception is if the scrub terminates upon an interval change. In that
 * scenario - the replicas discard their reservations on their own accord
 * when noticing the change in interval, and there is no need (and no
 * guaranteed way) to send them the release message.
 *
 * Timeouts:
 *
 *  Slow-Secondary Warning: (not re-implemented yet \todo)
 *
 *  Reservation Timeout:
 *  We limit the total time we wait for the replicas to respond to the
 *  reservation request. If the reservation back-and-forth does not complete
 *  within <conf> milliseconds, we give up and release all the reservations
 *  that have been acquired until that moment.
 *  (Why? because we have encountered instances where a reservation request was
 *  lost - either due to a bug or due to a network issue.)
 */
class ReplicaReservations {
  using clock = std::chrono::system_clock;

  ScrubMachineListener& m_scrubber;
  PG* m_pg;

  const pg_shard_t m_whoami;
  const spg_t m_pgid;

  /// used when queueing messages to the FSM
  OSDService* m_osds;

  /// the acting set (not including myself), sorted by OSD id
  std::vector<pg_shard_t> m_sorted_secondaries;

  /// the next replica to which we will send a reservation request
  std::vector<pg_shard_t>::const_iterator m_next_to_request;

  /// for detecting slow peers
  std::chrono::time_point<clock> m_request_sent_at;

  std::string m_log_msg_prefix;

 public:
  ReplicaReservations(ScrubMachineListener& scrubber);

  ~ReplicaReservations();

  /**
   * The received OK from the replica (after verifying that it is indeed
   * the replica we are expecting a reply from) is noted, and triggers
   * one of two: either sending a reservation request to the next replica,
   * or notifying the scrubber that we have reserved them all.
   */
  void handle_reserve_grant(OpRequestRef op, pg_shard_t from);

  void handle_reserve_reject(OpRequestRef op, pg_shard_t from);

  /**
   * if timing out on receiving replies from a replica:
   * All reserved replicas are released (including the one
   * that has not replied yet).
   * A failure notification is sent to the scrubber.
   */
  void handle_no_reply_timeout();

  /**
   * Notifies implementation that it is no longer responsible for releasing
   * tracked remote reservations.
   *
   * The intended usage is upon interval change.  In general, replicas are
   * responsible for releasing their own resources upon interval change without
   * coordination from the primary.
   *
   * Sends no messages.
   */
  void discard_remote_reservations();

  // note: 'public', as accessed via the 'standard' dout_prefix() macro
  std::ostream& gen_prefix(std::ostream& out) const;

 private:
  /// send a release message to that shard's OSD
  void release_replica(pg_shard_t peer, epoch_t epoch);

  /// notify the scrubber that we have failed to reserve replicas' resources
  void send_reject();

  /// send 'release' messages to all replicas we have managed to reserve
  void release_all();

  /// the only replica we are expecting a reply from
  std::optional<pg_shard_t> get_last_sent() const;

  /// The number of requests that have been sent (and not rejected) so far.
  size_t active_requests_cnt() const;

  /**
   * Either send a reservation request to the next replica, or notify the
   * scrubber that we have reserved all the replicas.
   */
  void send_next_reservation_or_complete();
};

}  // namespace Scrub
