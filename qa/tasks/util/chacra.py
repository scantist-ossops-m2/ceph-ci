#!/usr/bin/env python3

import argparse
import requests
import sys

from pathlib import Path
from urllib.parse import urlparse


PROJECT = 'ceph'
DISTRO = 'ubuntu'
DISTROVER = 'focal'
ARCH='x86_64'
BRANCHNAME = 'master'
SHA1 = 'latest'
FLAVOR = 'default'
FILENAME = 'cephadm'


def search(*args, **kwargs):
    '''
    Query shaman for a build result
    '''
    endpoint = 'https://shaman.ceph.com/api/search'
    resp = requests.get(endpoint, params=kwargs)
    resp.raise_for_status()
    return resp

def get_binary(
    host,
    project,
    ref,
    sha1,
    distro,
    distrover,
    arch,
    flavor,
    filename
):
    '''
    Pull a binary from chacra
    '''
    bin_url = f'https://{host}/binaries'
    bin_path = f'{project}/{ref}/{sha1}/{distro}/{distrover}/{arch}/flavors/{flavor}'
    bin_file = f'{bin_url}/{bin_path}/{filename}'

    resp = requests.get(bin_file, stream=True)
    resp.raise_for_status()
    return resp

def pull(
    project,
    distro,
    distrover,
    arch,
    branchname,
    sha1,
    flavor,
    src,
    dest,
):
    # query shaman for the built binary
    resp = search(project=project,
                  distros=_get_distros(distro, distrover, arch),
                  flavor=flavor,
                  ref=branchname,
                  sha1=sha1)

    resp_json = resp.json()
    if len(resp_json) == 0:
        raise RuntimeError(f'no results found at {resp.url}')

    # check the build status
    config = resp_json[0]
    status = config['status']
    if status != 'ready':
        raise RuntimeError(f'cannot pull file with status: {status}')
    # TODO: assert len() == 1?

    # pull the image from chacra
    chacra_host = urlparse(config['url']).netloc
    chacra_ref = config['ref']
    chacra_sha1 = config['sha1']
    print('got chacra host {}, ref {}, sha1 {} from {}'.format(
        chacra_host, chacra_ref, chacra_sha1, resp.url))

    resp = get_binary(
            chacra_host,
            config['project'],
            chacra_ref,
            chacra_sha1,
            config['distro'],
            config['distro_codename'],
            arch,
            config['flavor'],
            src,
    )
    print(f'got file from {resp.url}')

    dest = Path(dest).absolute()
    _write_binary(resp, dest)
    print(f'wrote binary file: {dest}')

def _write_binary(resp, dest):
    with open(dest, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
            print('.',)
            f.write(chunk)

def _get_distros(distro, distrover, arch=None):
    ret = f'{distro}/{distrover}'
    if arch:
        ret = f'{ret}/{arch}'
    return ret

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-P', default=PROJECT)
    parser.add_argument('--distro', '-D', default=DISTRO)
    parser.add_argument('--distrover', '-V', default=DISTROVER)
    parser.add_argument('--arch', '-a', default=ARCH)
    parser.add_argument('--branchname', '-b', default=BRANCHNAME)
    parser.add_argument('--sha1', '-s', default=SHA1)
    parser.add_argument('--flavor', default=FLAVOR)
    parser.add_argument('--src', '-s', default=FILENAME)
    parser.add_argument('--dest', '-d', default=FILENAME)
    args = parser.parse_args()

    pull(
        args.project,
        args.distro,
        args.distrover,
        args.arch,
        args.branchname,
        args.sha1,
        args.flavor,
        args.src,
        args.dest,
    )

    return 0


if __name__ == '__main__':
    sys.exit(main())
