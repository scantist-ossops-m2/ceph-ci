export interface CephfsSubvolume {
  name: string;
  info: CephfsSubvolumeInfo;
}

export interface CephfsSubvolumeInfo {
  mode: number;
  type: string;
  bytes_pcent: string;
  bytes_quota: string;
  data_pool: string;
  path: string;
  state: string;
  created_at: string;
  uid: number;
  gid: number;
  pool_namespace: string;
}
