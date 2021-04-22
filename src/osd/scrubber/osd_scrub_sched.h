// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <vector>

#include "common/RefCountedObj.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

#include "utime.h"

class PG;

namespace Scrub {

// possible outcome when trying to select a PG and scrub it
enum class attempt_t {
  scrubbing,	    // successfully started a scrub
  none_ready,	    // no pg to scrub
  local_resources,  // failure to secure local OSD scrub resource
  already_started,  // already started scrubbing this pg
  no_pg,	    // can't find this pg
  pg_state,	    // pg state (clean, active, etc.)
  preconditions	    // time, configuration, etc.
};

}  // namespace Scrub

/**
 * the ordered queue of PGs waiting to be scrubbed.
 * Main operations are scheduling/unscheduling a PG to be scrubbed at a certain time.
 *
 * A "penalty" queue maintains those PGs that have failed to reserve the resources
 * of their replicas. The PGs in this list will be reinstated into the scrub queue when
 * all eligible PGs were already handled, or after a timeout (or if their deadline
 * has passed [[disabled at this time]]).
 */
class ScrubQueue {
 public:
  enum class must_scrub_t { not_mandatory, mandatory };

  enum class qu_state_t {
    created,	    // the scrubber and its scrub-job were just created
    registered,     // in either of the two queues ('to_scrub' or 'penalized')
    unregistering,  // in the process of being unregistered. Will be finalized under lock
    not_registered, // not a primary, thus not considered for scrubbing by this OSD
    pg_deleted      // temporary state. Used when removing the PG.
  };

  ScrubQueue(CephContext* cct, OSDService& osds);

  struct TimeAndDeadline {
    utime_t scheduled_at;
    utime_t deadline;
  };

  struct ScrubJob final : public RefCountedObject {

    utime_t get_sched_time() const { return m_sched_time; }

    /// relatively low-cost(*) access to the scrub-job's state, to be used in logging.
    /// (*) not on x64 architecture
    string_view state_desc() const
    {
      return ScrubQueue::qu_state_text(m_state.load(std::memory_order_relaxed));
    }

    /// a time scheduled for scrub. The scrub could be delayed if system
    /// load is too high or if trying to scrub outside scrub hours
    utime_t m_sched_time;

    /// the hard upper bound of scrub time
    utime_t m_deadline;

    /// pg to be scrubbed
    const spg_t pgid;

    /// the OSD id (for the log)
    int whoami;

    std::atomic<qu_state_t> m_state{qu_state_t::created};

    /// the old 'is_registered'. Set whenever the job is registered with the OSD, i.e.
    /// is in either the 'to_scrub' or the 'penalized' vectors.
    std::atomic_bool m_in_queues{false};

    /// last scrub attempt failed in securing replica resources
    bool m_resources_failure{false};

    /**
     * m_updated is a temporary flag, used to create a barrier after m_sched_time
     * and 'm_deadline' (or other job entries) were modified by what might be a
     * different task.
     * m_updated also signals the need to move a job back from the penalized to the
     * regular queue.
     */
    std::atomic_bool m_updated{false};

    utime_t m_penalty_timeout{0, 0};

    CephContext* cct;

    ScrubJob(CephContext* cct,
	     const spg_t& pg,
	     int node_id);  // used when a PgScrubber is created

    void set_time_n_deadline(TimeAndDeadline ts)
    {
      m_sched_time = ts.scheduled_at;
      m_deadline = ts.deadline;
    }

    void dump(ceph::Formatter* f) const;

    /*
     * as the atomic 'm_in_q' appears in many log prints, accessing it for display-only
     * should be made less expensive (on ARM. On x86 the _relaxed produces the same cope
     * as '_cs')
     */
    std::string_view registration_state() const
    {
      return m_in_queues.load(std::memory_order_relaxed) ? " in-queue"sv : " not-queued"sv;
    }

    FRIEND_MAKE_REF(ScrubJob);
  };

  friend class TestOSDScrub;

  using ScrubJobRef = ceph::ref_t<ScrubJob>;
  using ScrubQContainer = std::vector<ceph::ref_t<ScrubJob>>;

  static string_view qu_state_text(qu_state_t st);

  /**
   * called periodically by the OSD to select the first scrub-eligible PG
   * and scrub it.
   *
   * Selection is affected by:
   * - time of day: scheduled scrubbing might be configured to only happen during certain
   *   hours;
   * - same for days of the week, and for the system load;
   *
   * @param preconds: what types are scrub are allowed, given system status & config
   * @return Scrub::attempt_t::scrubbing if a scrub session was successfully initiated.
   *   Otherwise - the failure cause.
   *
   * locking: locks jobs_lock
   */
  Scrub::attempt_t select_pg_and_scrub(Scrub::ScrubPreconds& preconds);

  /**
   * Translate attempt_ values into readable text
   */
  static string_view attempt_res_text(Scrub::attempt_t v);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(ceph::ref_t<ScrubJob> sjob);

  void final_rm_from_osd(ceph::ref_t<ScrubJob> scrub_job);

  /**
   * @return the list (not std::set!) of all scrub-jobs registered
   *   (apart from PGs in the process of being removed)
   */
  ScrubQContainer list_all_valid() const;

  struct sched_params_t {
    utime_t suggested_stamp{};
    double min_interval{0.0};
    double max_interval{0.0};
    must_scrub_t is_must{ScrubQueue::must_scrub_t::not_mandatory};
  };

  /**
   * Add the scrub-job to the list of PGs to be periodically
   * scrubbed by the OSD.
   * The registration is active as long as the PG exists and the OSD is its primary.
   * add to the OSD's to_scrub queue
   *
   * See update_job() for the handling of the 'suggested' parameter.
   *
   * locking: might lock jobs_lock
   */
  void register_with_osd(ceph::ref_t<ScrubJob> sjob, const sched_params_t& suggested);

  /**
   * modify the scrub-job of a specific PG
   *
   * There are 3 argument combinations to consider:
   * - 'must' is asserted, and the suggested time is 'scrub_must_stamp':
   *   the registration will be with "beginning of time" target, making the
   *   PG eligible to immediate scrub (given that external conditions do not
   *   prevent scrubbing)
   *
   * - 'must' is asserted, and the suggested time is 'now':
   *   This happens if our stats are unknown. The results is similar to the previous
   *   scenario.
   *
   * - not a 'must': we take the suggested time as a basis, and add to it some
   *   configuration / random delays.
   *
   *   locking: not using the jobs_lock
   */
  void update_job(ceph::ref_t<ScrubJob> sjob, const sched_params_t& suggested);

 public:
  void dump_scrubs(ceph::Formatter* f) const;

  /**
   * No new scrub session will start while a scrub was initiate on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  bool can_inc_scrubs() const;
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();
  void dump_scrub_reservations(ceph::Formatter* f) const;

  /**
   * Pacing the scrub operation by inserting delays (mostly between chunks)
   *
   * Special handling for regular scrubs that continued into "no scrub" times. Scrubbing
   * will continue, but the delays will be controlled by a separate (read - with higher
   * value) configuration element (osd_scrub_extended_sleep).
   */
  double scrub_sleep_time(bool must_scrub) const;  /// \todo (future) return milliseconds

  /**
   *  called every heartbeat to update the "daily" load average
   *
   *  @returns a load value for the logger
   */
  [[nodiscard]] std::optional<double> update_load_average();

 private:
  CephContext* m_cct;
  OSDService& m_osds;

  /// scrub resources management lock
  mutable ceph::mutex resource_lock = ceph::make_mutex("ScrubQueue::resource_lock");

  /// job queues lock
  mutable std::timed_mutex jobs_lock;

  ScrubQContainer to_scrub;   ///< scrub-jobs (i.e. PGs) to scrub
  ScrubQContainer penalized;  ///< those that failed to reserve resources
  bool restore_penalized{false};

  double daily_loadavg{0.0};

  struct time_indirect_t {
    bool operator()(const ceph::ref_t<ScrubJob>& lhs,
		    const ceph::ref_t<ScrubJob>& rhs) const
    {
      return lhs->m_sched_time < rhs->m_sched_time;
    }
  };

  static inline constexpr auto valid_job = [](const auto& jobref) -> bool {
    return jobref->m_state == qu_state_t::registered;
  };

  static inline constexpr auto invalid_state = [](const auto& jobref) -> bool {
    return jobref->m_state == qu_state_t::pg_deleted || jobref->m_state == qu_state_t::not_registered;
  };

  static inline constexpr auto clear_upd_flag = [](const auto& jobref) -> void {
    jobref->m_updated = false;
  };

  /**
   * Are there scrub-jobs that should be reinstated?
   */
  void scan_penalized(bool forgive_all, utime_t time_now);

  /**
   * clear dead entries (unregistered, or belonging to removed PGs) from a queue.
   * Job state is changed to match new status.
   */
  void clear_dead_jobs(ScrubQContainer& group);

  ScrubQContainer collect_ripe_jobs(ScrubQContainer& group, utime_t time_now);


  // the counters used to manage scrub activity parallelism:
  int m_scrubs_local{0};
  int m_scrubs_remote{0};

  std::atomic_bool a_pg_is_reserving{false};

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit(utime_t now) const;

  /**
   * If the scrub job was not explicitly requested, we postpone it by some
   * random length of time.
   * And if delaying the scrub - we calculate, based on pool parameters, a deadline
   * we should scrub before.
   *
   * @return a pair of values: the determined scrub time, and the deadline
   */
  TimeAndDeadline adjust_target_time(const sched_params_t& recomputed_params) const;

  /**
   * Look for scrub-jobs that have their 'm_resources_failure' set. These jobs
   * have failed to acquire remote resources last time we've initiated a scrub session
   * on them. They are now moved from the 'to_scrub' queue to the 'penalized' set.
   *
   * locking: called with job_lock held
   */
  void move_failed_pgs(utime_t now_is);

  Scrub::attempt_t select_from_group(ScrubQContainer& group,
				     const Scrub::ScrubPreconds& preconds,
				     utime_t now_is);
};
