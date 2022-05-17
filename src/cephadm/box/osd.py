import json
import os
import re
from typing import Dict

from util import (
    Config,
    Target,
    ensure_inside_container,
    ensure_outside_container,
    get_orch_hosts,
    inside_container,
    run_cephadm_shell_command,
    run_dc_shell_command,
    run_shell_command,
)


DEVICES_FILE="./devices.json"


def create_loopback_devices(osds: int) -> Dict:
    assert osds
    cleanup()
    osd_devs = dict()
    for i in range(osds):
        img_name = f'osd{i}'
        loop_dev = create_loopback_device(img_name)
        osd_devs[i] = dict(img_name=img_name, device=loop_dev)
    with open(DEVICES_FILE, 'w') as dev_file:
        dev_file.write(json.dumps(osd_devs))
    return osd_devs


def create_loopback_device(img_name, size_gb=5):
    loop_img_dir = Config.get('loop_img_dir')
    run_shell_command(f'mkdir -p {loop_img_dir}')
    loop_img = os.path.join(loop_img_dir, img_name)
    run_shell_command(f'rm -f {loop_img}')
    run_shell_command(f'dd if=/dev/zero of={loop_img} bs=1 count=0 seek={size_gb}G')
    loop_dev = run_shell_command(f'sudo losetup -f')
    if not os.path.exists(loop_dev):
        dev_minor = re.match(r'\/dev\/[^\d]+(\d+)', loop_dev).groups()[0]
        run_shell_command(f'sudo mknod -m777 {loop_dev} b 7 {dev_minor}')
    if os.path.ismount(loop_dev):
        os.umount(loop_dev)
    run_shell_command(f'sudo losetup {loop_dev} {loop_img}')
    return loop_dev


def create_scsi_devices(count, size_gb=6):
    cleanup()
    size_mb = size_gb * 1024
    run_shell_command(
        f'sudo modprobe scsi_debug add_host={count} dev_size_mb={size_mb}')
    devices = run_shell_command("lsscsi | grep scsi_debug | awk '{print $6}'")\
        .splitlines()
    osd_devs = dict()
    for i in range(count):
        osd_devs[i] = dict(device=devices[i])
    run_shell_command(f"sudo chmod 777 {' '.join(devs)}")
    with open(DEVICES_FILE, 'w') as dev_file:
        dev_file.write(json.dumps(osd_devs))
    return devs


def load_osd_devices():
    if not os.path.exists(DEVICES_FILE):
        return dict()
    with open(DEVICES_FILE) as dev_file:
        devs = json.loads(dev_file.read())
    return devs


@ensure_inside_container
def deploy_osd(data: str, hostname: str) -> bool:
    out = run_cephadm_shell_command(f'ceph orch daemon add osd "{hostname}:{data}"')
    return 'Created osd(s)' in out


def cleanup() -> None:
    run_shell_command('sudo rmmod scsi_debug', expect_error=True)
    loop_img_dir = Config.get('loop_img_dir')
    osd_devs = load_osd_devices()
    for osd in osd_devs.values():
        device = osd['device']
        if 'loop' in device:
            loop_img = os.path.join(loop_img_dir, osd['img_name'])
            run_shell_command(f'sudo losetup -d {device}')
            os.remove(loop_img)
    run_shell_command(f'rm -rf {loop_img_dir}')


def deploy_osds(count):
    if inside_container():
        print('xd')
        return
    osd_devs = load_osd_devices()
    hosts = get_orch_hosts()
    host_index = 0
    v = '-v' if Config.get('verbose') else ''
    for osd in osd_devs.values():
        deployed = False
        while not deployed:
            hostname = hosts[host_index]['hostname']
            deployed = run_dc_shell_command(
                f'/cephadm/box/box.py {v} osd deploy --data {osd["device"]} --hostname {hostname}',
                1,
                'seed'
            )
            deployed = 'created osd' in deployed.lower()
        host_index = (host_index + 1) % len(hosts)


class Osd(Target):
    _help = """
    Deploy osds and create needed block devices with loopback devices:
    Actions:
    - deploy: Deploy an osd given a block device
    - create_loop: Create needed loopback devices and block devices in logical volumes
    for a number of osds.
    """
    actions = ['deploy', 'create_loop']

    def set_args(self):
        self.parser.add_argument('action', choices=Osd.actions)
        self.parser.add_argument('--data', type=str, help='path to a block device')
        self.parser.add_argument('--hostname', type=str, help='host to deploy osd')
        self.parser.add_argument('--osds', type=int, default=0, help='number of osds')
        self.parser.add_argument(
            '--osd-type', choices=['loop', 'scsi_debug'], default='loop',
            help='the type of device to use'
        )
        self.parser.add_argument(
            '--vg', type=str, help='Deploy with all lv from virtual group'
        )

    def deploy(self):
        data = Config.get('data')
        hostname = Config.get('hostname')
        if not hostname:
            # assume this host
            hostname = run_shell_command('hostname')
        if not data:
            deploy_osds(Config.get('osds'))
        else:
            deploy_osd(data, hostname)


    @ensure_outside_container
    def create_loop(self):
        osds = Config.get('osds')
        osd_type = Config.get('osd_type')
        if osd_type == 'loop':
            create_loopback_devices(int(osds))
        elif osd_type == 'scsi_debug':
            create_scsi_devices(int(osds))
        print('Successfully added logical volumes in loopback devices')
