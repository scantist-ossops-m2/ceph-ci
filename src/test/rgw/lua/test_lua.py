import logging
import json
import tempfile
import random
import threading
import subprocess
import socket
import time
import os
import string
import boto
from botocore.exceptions import ClientError
from http import server as http_server
from random import randint
import hashlib
from nose.plugins.attrib import attr
import boto3
import datetime
from dateutil import parser

from boto.s3.connection import S3Connection

from . import(
    get_config_host,
    get_config_port,
    get_access_key,
    get_secret_key
    )

from .api import delete_all_objects, \
    admin

from nose import SkipTest
from nose.tools import assert_not_equal, assert_equal, assert_in
import boto.s3.tagging

# configure logging for the tests module
log = logging.getLogger(__name__)


num_buckets = 0
run_prefix=''.join(random.choice(string.ascii_lowercase) for _ in range(6))

def gen_bucket_name():
    global num_buckets

    num_buckets += 1
    return run_prefix + '-' + str(num_buckets)


def set_contents_from_string(key, content):
    try:
        key.set_contents_from_string(content)
    except Exception as e:
        print('Error: ' + str(e))


def get_ip():
    return 'localhost'


def get_ip_http():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # address should not be reachable
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def connection():
    hostname = get_config_host()
    port_no = get_config_port()
    vstart_access_key = get_access_key()
    vstart_secret_key = get_secret_key()

    conn = S3Connection(aws_access_key_id=vstart_access_key,
                  aws_secret_access_key=vstart_secret_key,
                      is_secure=False, port=port_no, host=hostname, 
                      calling_format='boto.s3.connection.OrdinaryCallingFormat')

    return conn


def connection2():
    hostname = get_config_host()
    port_no = 8001
    vstart_access_key = get_access_key()
    vstart_secret_key = get_secret_key()

    conn = S3Connection(aws_access_key_id=vstart_access_key,
                  aws_secret_access_key=vstart_secret_key,
                      is_secure=False, port=port_no, host=hostname, 
                      calling_format='boto.s3.connection.OrdinaryCallingFormat')

    return conn


def connection3():
    hostname = get_config_host()
    port_no = get_config_port()
    vstart_access_key = get_access_key()
    vstart_secret_key = get_secret_key()

    conn = boto3.client(service_name="s3",
            aws_access_key_id=vstart_access_key,
            aws_secret_access_key=vstart_secret_key,
            endpoint_url="http://"+hostname+":"+str(port_no))

    return conn


def another_user(tenant=None):
    access_key = str(time.time())
    secret_key = str(time.time())
    uid = 'superman' + str(time.time())
    if tenant:
        _, result = admin(['user', 'create', '--uid', uid, '--tenant', tenant, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])  
    else:
        _, result = admin(['user', 'create', '--uid', uid, '--access-key', access_key, '--secret-key', secret_key, '--display-name', '"Super Man"'])  

    assert_equal(result, 0)
    conn = S3Connection(aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                      is_secure=False, port=get_config_port(), host=get_config_host(), 
                      calling_format='boto.s3.connection.OrdinaryCallingFormat')
    return conn


def put_script(script, context, tenant=None):
    fp = tempfile.NamedTemporaryFile(mode='w+')
    fp.write(script)
    fp.flush()
    if tenant:
        result = admin(['script', 'put', '--infile', fp.name, '--context', context, '--tenant', tenant])
    else:
        result = admin(['script', 'put', '--infile', fp.name, '--context', context])

    fp.close()
    return result

class UnixSocket:
    def __init__(self, socket_path):
        self.socket_path = socket_path
        self.stop = False
        self.started = False
        self.events = []
        self.t = threading.Thread(target=self.listen_on_socket)
        self.t.start()
        while not self.started:
            print("UnixSocket: waiting for unix socket server to start")
            time.sleep(1)

    def shutdown(self):
        self.stop = True
        self.t.join()

    def get_and_reset_events(self):
        tmp = self.events
        self.events = []
        return tmp

    def listen_on_socket(self):
        self.started = True
        # remove the socket file if it already exists
        try:
            os.unlink(self.socket_path)
        except OSError:
            if os.path.exists(self.socket_path):
                raise

        # create and bind the Unix socket server
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self.socket_path)

        # listen for incoming connections
        server.listen(1)
        # accept timeout is 30s at the beginning
        server.settimeout(30)
        print("UnixSocket '%s' is listening for incoming connections..." % self.socket_path)

        while not self.stop:
            # accept connections
            try:
                connection, _ = server.accept()
            except Exception as e:
                print("UnixSocket: accept "+str(e))
                continue
            # after we start accept/recv timeouts are 5s
            server.settimeout(5)
            connection.settimeout(5)

            try:
                print("UnixSocket: new connection accepted")
                # receive data from the client
                while True:
                    # recv timeout is 5s
                    data = connection.recv(1024)
                    if not data:
                        break
                    event = json.loads(data.decode())
                    self.events.append(event)
            finally:
                # close the connection
                connection.close()
                print("UnixSocket: connection closed")

        # remove the socket file
        os.unlink(self.socket_path)


#####################
# lua scripting tests
#####################


@attr('basic_test')
def test_script_management():
    contexts = ['prerequest', 'postrequest', 'background', 'getdata', 'putdata']
    scripts = {}
    for context in contexts:
        script = 'print("hello from ' + context + '")'
        result = put_script(script, context)
        assert_equal(result[1], 0)
        scripts[context] = script
    for context in contexts:
        result = admin(['script', 'get', '--context', context])
        assert_equal(result[1], 0)
        assert_equal(result[0].strip(), scripts[context])
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert_equal(result[1], 0)
    for context in contexts:
        result = admin(['script', 'get', '--context', context])
        assert_equal(result[1], 0)
        assert_equal(result[0].strip(), 'no script exists for context: ' + context)


@attr('basic_test')
def test_script_management_with_tenant():
    tenant = 'mytenant'
    conn2 = another_user(tenant)
    contexts = ['prerequest', 'postrequest', 'getdata', 'putdata']
    scripts = {}
    for context in contexts:
        for t in ['', tenant]:
            script = 'print("hello from ' + context + ' and ' + tenant + '")'
            result = put_script(script, context, t)
            assert_equal(result[1], 0)
            scripts[context+t] = script
    for context in contexts:
        result = admin(['script', 'get', '--context', context])
        assert_equal(result[1], 0)
        assert_equal(result[0].strip(), scripts[context])
        result = admin(['script', 'rm', '--context', context])
        assert_equal(result[1], 0)
        result = admin(['script', 'get', '--context', context])
        assert_equal(result[1], 0)
        assert_equal(result[0].strip(), 'no script exists for context: ' + context)
        result = admin(['script', 'get', '--context', context, '--tenant', tenant])
        assert_equal(result[1], 0)
        assert_equal(result[0].strip(), scripts[context+tenant])
        result = admin(['script', 'rm', '--context', context, '--tenant', tenant])
        assert_equal(result[1], 0)
        result = admin(['script', 'get', '--context', context, '--tenant', tenant])
        assert_equal(result[1], 0)
        assert_equal(result[0].strip(), 'no script exists for context: ' + context + ' in tenant: ' + tenant)


@attr('request_test')
def test_put_obj():
    script = '''
RGWDebugLog("op was: "..Request.RGWOp)
if Request.RGWOp == "put_obj" then
    local object = Request.Object
    local message = Request.bucket.Name .. "," .. object.Name .. 
        "," .. object.Id .. "," .. object.Size .. "," .. object.MTime
    RGWDebugLog("set: x-amz-meta-test to: " .. message)
    Request.HTTP.Metadata["x-amz-meta-test"] = message
end
'''
    context = "prerequest"
    result = put_script(script, context)
    assert_equal(result[1], 0)
	
    conn = connection3()
    bucket_name = gen_bucket_name()
    conn.create_bucket(Bucket=bucket_name)
    key = "hello"
    conn.put_object(Body="1234567890".encode("ascii"), Bucket=bucket_name, Key=key)

    result = conn.get_object(Bucket=bucket_name, Key=key)
    message = result['ResponseMetadata']['HTTPHeaders']['x-amz-meta-test']
    assert_equal(message, bucket_name+","+key+","+key+",0,1970-01-01 00:00:00")

    # cleanup
    conn.delete_object(Bucket=bucket_name, Key=key)
    conn.delete_bucket(Bucket=bucket_name)
    contexts = ['prerequest', 'postrequest', 'getdata', 'putdata']
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert_equal(result[1], 0)


@attr('example_test')
def test_copyfrom():
    script = '''
function print_object(object)
    RGWDebugLog("  Name: " .. object.Name)
    RGWDebugLog("  Instance: " .. object.Instance)
    RGWDebugLog("  Id: " .. object.Id)
    RGWDebugLog("  Size: " .. object.Size)
    RGWDebugLog("  MTime: " .. object.MTime)
end

if Request.CopyFrom and Request.Object and Request.CopyFrom.Object then
    RGWDebugLog("copy from object:")
    print_object(Request.CopyFrom.Object)
    RGWDebugLog("to object:")
    print_object(Request.Object)
end
RGWDebugLog("op was: "..Request.RGWOp)
'''

    contexts = ['prerequest', 'postrequest', 'getdata', 'putdata']
    for context in contexts:
        footer = '\nRGWDebugLog("context was: '+context+'\\n\\n")'
        result = put_script(script+footer, context)
        assert_equal(result[1], 0)
	
    conn = connection()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn.create_bucket(bucket_name)
    # create objects in the bucket (async)
    number_of_objects = 5
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        bucket.copy_key('copyof'+key.name, bucket.name, key.name)

    # cleanup
    delete_all_objects(conn, bucket_name)
    conn.delete_bucket(bucket_name)
    contexts = ['prerequest', 'postrequest', 'getdata', 'putdata']
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert_equal(result[1], 0)


@attr('example_test')
def test_entropy():
    script = '''
function object_entropy()
    local byte_hist = {}
    local byte_hist_size = 256
    for i = 1,byte_hist_size do
        byte_hist[i] = 0
    end
    local total = 0

    for i, c in pairs(Data)  do
        local byte = c:byte() + 1
        byte_hist[byte] = byte_hist[byte] + 1
        total = total + 1
    end

    entropy = 0

    for _, count in ipairs(byte_hist) do
        if count ~= 0 then
            local p = 1.0 * count / total
            entropy = entropy - (p * math.log(p)/math.log(byte_hist_size))
        end
    end

    return entropy
end

local full_name = Request.Bucket.Name.."-"..Request.Object.Name
RGWDebugLog("entropy of chunk of: " .. full_name .. " at offset: " .. tostring(Offset)  ..  " is: " .. tostring(object_entropy()))
RGWDebugLog("payload size of chunk of: " .. full_name .. " is: " .. #Data)
'''

    result = put_script(script, "putdata")
    assert_equal(result[1], 0)

    conn = connection()
    bucket_name = gen_bucket_name()
    # create bucket
    bucket = conn.create_bucket(bucket_name)
    # create objects in the bucket (async)
    number_of_objects = 5
    client_threads = []
    start_time = time.time()
    for i in range(number_of_objects):
        key = bucket.new_key(str(i))
        content = str(os.urandom(1024*1024*16))
        thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
        thr.start()
        client_threads.append(thr)
    [thr.join() for thr in client_threads]

    # cleanup
    delete_all_objects(conn, bucket_name)
    conn.delete_bucket(bucket_name)
    contexts = ['prerequest', 'postrequest', 'background', 'getdata', 'putdata']
    for context in contexts:
        result = admin(['script', 'rm', '--context', context])
        assert_equal(result[1], 0)


@attr('example_test')
def test_access_log():
    bucket_name = gen_bucket_name()
    socket_path = '/tmp/'+bucket_name

    script = '''
if Request.RGWOp == "get_obj" then
    local json = require("cjson")
    local socket = require("socket")
    local unix = require("socket.unix")
    local s = unix()
    E = {{}}

    msg = {{bucket = (Request.Bucket or (Request.CopyFrom or E).Bucket).Name,
        object = Request.Object.Name,
        time = Request.Time,
        operation = Request.RGWOp,
        http_status = Request.Response.HTTPStatusCode,
        error_code = Request.Response.HTTPStatus,
        object_size = Request.Object.Size,
        trans_id = Request.TransactionId}}

    assert(s:connect("{}"))
    s:send(json.encode(msg).."\\n")
    s:close()
end
'''.format(socket_path)

    result = admin(['script-package', 'add', '--package=lua-cjson', '--allow-compilation'])
    assert_equal(result[1], 0)
    result = admin(['script-package', 'add', '--package=luasocket', '--allow-compilation'])
    assert_equal(result[1], 0)
    result = admin(['script-package', 'reload'])
    assert_equal(result[1], 0)
    result = put_script(script, "postrequest")
    assert_equal(result[1], 0)

    socket_server = UnixSocket(socket_path)
    try:
        conn = connection()
        # create bucket
        bucket = conn.create_bucket(bucket_name)
        # create objects in the bucket (async)
        number_of_objects = 5
        client_threads = []
        start_time = time.time()
        keys = []
        for i in range(number_of_objects):
            key = bucket.new_key(str(i))
            keys.append(str(i))
            content = str(os.urandom(1024*1024))
            thr = threading.Thread(target = set_contents_from_string, args=(key, content,))
            thr.start()
            client_threads.append(thr)
        [thr.join() for thr in client_threads]

        for key in bucket.list():
            conn.get_bucket(bucket_name).get_key(key.name)

        time.sleep(5)
        event_keys = []
        for event in socket_server.get_and_reset_events():
            assert_equal(event['bucket'], bucket_name)
            event_keys.append(event['object'])

        assert_equal(keys, event_keys)

    finally:
        socket_server.shutdown()
        delete_all_objects(conn, bucket_name)
        conn.delete_bucket(bucket_name)
        contexts = ['prerequest', 'postrequest', 'background', 'getdata', 'putdata']
        for context in contexts:
            result = admin(['script', 'rm', '--context', context])
            assert_equal(result[1], 0)

