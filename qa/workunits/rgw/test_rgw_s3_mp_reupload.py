import boto3
import sys
import os
import subprocess

#boto3.set_stream_logger(name='botocore')

rgw_host = os.environ['RGW_HOST']
rgw_port = int(os.environ['RGW_PORT'])
access_key = os.environ['RGW_ACCESS_KEY']
secret_key = os.environ['RGW_SECRET_KEY']

endpoint='http://%s:%d' % (rgw_host, rgw_port)

client = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)

if len(sys.argv) >= 2:
    bucket_name = sys.argv[1]
else:
    bucket_name = "bkt314"

key = "mpu_test4"
nparts = 2
ndups = 31
complete_mpu = True
do_reupload = True

part_path = "/tmp/mp_part_5m"
subprocess.run(["dd", "if=/dev/urandom", "of=" + part_path, "bs=1M", "count=5"], check=True)

if False:
    try:
        res = client.create_bucket(Bucket=bucket_name)
    except:
        pass
else:
    res = client.create_bucket(Bucket=bucket_name)

f = open(part_path, 'rb')

res = client.create_multipart_upload(Bucket=bucket_name, Key=key)
mpu_id = res["UploadId"]

print("start UploadId=%s" % (mpu_id))

parts = []
parts2 = []

for ix in range(0,nparts):
    part_num = ix + 1
    f.seek(0)
    res = client.upload_part(Body=f, Bucket=bucket_name, Key=key,
                             UploadId=mpu_id, PartNumber=part_num)
    # save
    etag = res['ETag']
    part = {'ETag': etag, 'PartNumber': part_num}
    print("phase 1 uploaded part %s" % part)
    parts.append(part)

if do_reupload:
    # just re-upload part 1
    part_num = 1
    for ix in range(0,ndups):
        f.seek(0)
        res = client.upload_part(Body=f, Bucket=bucket_name, Key=key,
                                UploadId=mpu_id, PartNumber=part_num)
        etag = res['ETag']
        part = {'ETag': etag, 'PartNumber': part_num}
        print ("phase 2 uploaded part %s" % part)

        # save
        etag = res['ETag']
        part = {'ETag': etag, 'PartNumber': part_num}
        parts2.append(part)

if complete_mpu:
    print("completing multipart upload, parts=%s" % parts)
    res = client.complete_multipart_upload(
        Bucket=bucket_name, Key=key, UploadId=mpu_id,
        MultipartUpload={'Parts': parts})

# clean up
subprocess.run(["rm", "-f", part_path], check=True)
