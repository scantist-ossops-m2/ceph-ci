==============
Upgrading Ceph
==============

.. DANGER:: DATE: 01 NOV 2021. 

   DO NOT UPGRADE TO CEPH PACIFIC FROM AN OLDER VERSION.  

   A recently-discovered bug (https://tracker.ceph.com/issues/53062) can cause
   data corruption. This bug occurs during OMAP format conversion for
   clusters that are updated to Pacific. New clusters are not affected by this
   bug.

   The trigger for this bug is BlueStore's repair/quick-fix functionality. This
   bug can be triggered in two known ways: 

    (1) manually via the ceph-bluestore-tool, or 
    (2) automatically, by OSD if ``bluestore_fsck_quick_fix_on_mount`` is set 
        to true.

   The fix for this bug is expected to be available in Ceph v16.2.7.

   DO NOT set ``bluestore_quick_fix_on_mount`` to true. If it is currently
   set to true in your configuration, immediately set it to false.

   DO NOT run ``ceph-bluestore-tool``'s repair/quick-fix commands.

Cephadm can safely upgrade Ceph from one bugfix release to the next.  For
example, you can upgrade from v15.2.0 (the first Octopus release) to the next
point release, v15.2.1.

The automated upgrade process follows Ceph best practices.  For example:

* The upgrade order starts with managers, monitors, then other daemons.
* Each daemon is restarted only after Ceph indicates that the cluster
  will remain available.

.. note::

   The Ceph cluster health status is likely to switch to
   ``HEALTH_WARNING`` during the upgrade.

.. note:: 

   In case a host of the cluster is offline, the upgrade is paused.


Starting the upgrade
====================

Before you use cephadm to upgrade Ceph, verify that all hosts are currently online and that your cluster is healthy by running the following command:

.. prompt:: bash #

   ceph -s

To upgrade (or downgrade) to a specific release, run the following command:

.. prompt:: bash #

  ceph orch upgrade start --ceph-version <version>

For example, to upgrade to v16.2.6, run the following command:

.. prompt:: bash #

  ceph orch upgrade start --ceph-version 16.2.6

.. note::

    From version v16.2.6 the Docker Hub registry is no longer used, so if you use Docker you have to point it to the image in the quay.io registry:

.. prompt:: bash #

  ceph orch upgrade start --image quay.io/ceph/ceph:v16.2.6


Monitoring the upgrade
======================

Determine (1) whether an upgrade is in progress and (2) which version the
cluster is upgrading to by running the following command:

.. prompt:: bash #

  ceph orch upgrade status

Watching the progress bar during a Ceph upgrade
-----------------------------------------------

During the upgrade, a progress bar is visible in the ceph status output. It
looks like this:

.. code-block:: console

  # ceph -s

  [...]
    progress:
      Upgrade to docker.io/ceph/ceph:v15.2.1 (00h 20m 12s)
        [=======.....................] (time remaining: 01h 43m 31s)

Watching the cephadm log during an upgrade
------------------------------------------

Watch the cephadm log by running the following command:

.. prompt:: bash #

  ceph -W cephadm


Canceling an upgrade
====================

You can stop the upgrade process at any time by running the following command:

.. prompt:: bash #

  ceph orch upgrade stop


Potential problems
==================

There are a few health alerts that can arise during the upgrade process.

UPGRADE_NO_STANDBY_MGR
----------------------

This alert (``UPGRADE_NO_STANDBY_MGR``) means that Ceph does not detect an
active standby manager daemon. In order to proceed with the upgrade, Ceph
requires an active standby manager daemon (which you can think of in this
context as "a second manager").

You can ensure that Cephadm is configured to run 2 (or more) managers by
running the following command:

.. prompt:: bash #

  ceph orch apply mgr 2  # or more

You can check the status of existing mgr daemons by running the following
command:

.. prompt:: bash #

  ceph orch ps --daemon-type mgr

If an existing mgr daemon has stopped, you can try to restart it by running the
following command: 

.. prompt:: bash #

  ceph orch daemon restart <name>

UPGRADE_FAILED_PULL
-------------------

This alert (``UPGRADE_FAILED_PULL``) means that Ceph was unable to pull the
container image for the target version. This can happen if you specify a
version or container image that does not exist (e.g. "1.2.3"), or if the
container registry can not be reached by one or more hosts in the cluster.

To cancel the existing upgrade and to specify a different target version, run
the following commands: 

.. prompt:: bash #

  ceph orch upgrade stop
  ceph orch upgrade start --ceph-version <version>


Using customized container images
=================================

For most users, upgrading requires nothing more complicated than specifying the
Ceph version number to upgrade to.  In such cases, cephadm locates the specific
Ceph container image to use by combining the ``container_image_base``
configuration option (default: ``docker.io/ceph/ceph``) with a tag of
``vX.Y.Z``.

But it is possible to upgrade to an arbitrary container image, if that's what
you need. For example, the following command upgrades to a development build:

.. prompt:: bash #

  ceph orch upgrade start --image quay.io/ceph-ci/ceph:recent-git-branch-name

For more information about available container images, see :ref:`containers`.

Staggered Upgrade
=================

Some users may prefer to have their upgrade be done in shorter bursts rather
than one fire-and-forget upgrade command. The upgrade command, starting
in 16.2.9 and 17.2.1 allows specifying parameters to limit which daemons are
upgraded by a single upgrade command. The options in include ``daemon_types``,
``services`` and ``hosts``. ``daemon_types`` takes a comma separated list
of daemon types and will only upgrade daemons of those types. ``services``
is mutually exclusive with ``daemon_types``, only takes services of one type
at a time (e.g. can't provide an osd and rgw service at the same time) and
will only upgrade daemons belonging to those services. ``hosts`` can be combined
with ``daemon_types`` or ``services`` or provided on its own. The argument
follows the same format as the command line options for :ref:`orchestrator-cli-placement-spec`.

Example, specifying daemon types and hosts:

.. prompt:: bash #

  ceph orch upgrade start --image <image-name> --daemon-types mgr,mon --hosts host1,host2

Example, specifying services:

.. prompt:: bash #

  ceph orch upgrade start --image <image-name> --services rgw.example1,rgw.example2

.. note::

   cephadm strictly enforces an order to the upgrade of daemons that is still present
   in staggered upgrade scenarios. The current upgrade ordering is
   ``mgr -> mon -> crash -> osd -> mds -> rgw -> rbd-mirror -> cephfs-mirror -> iscsi -> nfs``.
   If you attempt to specify parameters that would force us to upgrade daemons out of
   order, the upgrade command will block and inform you of which daemons earlier
   in the upgrade order will be missed using your parameters.

.. note::

   In order to verify the upgrade order will not be broken, upgrade commands with
   extra limiting parameters must first validate the options before beginning the
   upgrade. This may require pulling the image you are trying to upgrade to. Do
   not be surprised if the upgrade start command takes a while to return when limiting
   parameters are provided.

.. note::

   In staggered upgrade scenarios (when a limiting parameter is provide) monitoring
   stack daemons such as prometheus or node-exporter are "upgraded" (if you are
   not using the default image or if the default does not change between upgrade
   versions it's actually just a redeploy) after the mgr daemons are finished
   upgrading. Do not be surprised if you attempt to upgrade mgr daemons and it
   takes longer than expected as it may just be upgrading monitoring stack daemons
   as well.

Upgrading to a version that supports staggered upgrade from one that doesn't
----------------------------------------------------------------------------

While upgrading from a version that already supports staggered upgrades the process
simply requires providing the necessary arguments. However, if you wish to upgrade
TO a version that supports staggered upgrade from one that does not, there is a
workaround that can be used. It requires first manually upgrading the mgr daemons
and then passing the limiting parameters normally from that point on.

.. warning::
  Make sure you have multiple running mgr daemons before attempting this procedure.

To start with, determine which mgr is your active one and which are standby. This
can be done in a variety of ways such as looking at the ``ceph -s`` output. Then,
manually upgrade each standby mgr daemon with:

.. prompt:: bash #

  ceph orch daemon redeploy mgr.example1.abcdef --image <new-image-name>

At this point, a mgr fail over should allow us to have the active mgr be one
running the new version.

.. prompt:: bash #

  ceph mgr fail

Verify the active mgr is now one running the new version. To complete the mgr
upgrading:

.. prompt:: bash #

  ceph orch upgrade start --image <new-image-name> --daemon-types mgr

You should now have all your mgr daemons on the new version and be able to
make use of the limiting parameters for the rest of the upgrade.
