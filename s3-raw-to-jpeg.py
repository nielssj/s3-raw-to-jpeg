__author__ = 'niels'

import sys, re, time, os
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from subprocess import call

conn = S3Connection(sys.argv[1], sys.argv[2])
bucket = conn.get_bucket("owncloud-photos")

content = bucket.list(prefix="Kina/20141117")

class KeyMeta:
    s3_path = ""
    filename = ""
    filename_tiny = ""
    filename_prev = ""
    filename_full = ""
    def __init__(self, s3_path, filename):
        self.s3_path = s3_path
        self.filename = filename

def getRawFromS3(key):
    # Get filename from path
    req = re.search('^(.+/)(.+).NEF$', key.name)
    if(req):
        s3_path = req.group(1)
        filename = req.group(2)

        # Copy to file
        fp = open(filename + ".NEF", "w")
        key.get_file(fp)
        return KeyMeta(s3_path, filename)
    else:
        return None

def produceJPGs(key_meta):
    params_def = ["ufraw-batch", key_meta.filename + ".NEF", "--embedded-image", "--overwrite"]

    # Tiny thumbnail (250px)
    key_meta.filename_tiny = key_meta.filename + "_250.jpg"
    params_tiny = list(params_def)
    params_tiny.extend(["--size=250", "--output=" + key_meta.filename_tiny])
    call(params_tiny)

    # Preview (1600px)
    key_meta.filename_prev = key_meta.filename + "_1600.jpg"
    params_prev = list(params_def)
    params_prev.extend(["--size=1600", "--output=" + key_meta.filename_prev])
    call(params_prev)

    # Full
    key_meta.filename_full = key_meta.filename + ".jpg"
    params_full = list(params_def)
    params_full.extend(["--output=" + key_meta.filename_full])
    call(params_full)

    return key_meta

def uploadToS3(key_meta):
    # Tiny
    key_tiny = Key(bucket)
    key_tiny.key = key_meta.s3_path + key_meta.filename_tiny
    key_tiny.set_contents_from_filename(key_meta.filename_tiny)
    print "[%s] uploaded" % key_tiny.key

    # Preview
    key_prev = Key(bucket)
    key_prev.key = key_meta.s3_path + key_meta.filename_prev
    key_prev.set_contents_from_filename(key_meta.filename_prev)
    print "[%s] uploaded" % key_prev.key

    # Full
    key_full = Key(bucket)
    key_full.key = key_meta.s3_path + key_meta.filename_full
    key_full.set_contents_from_filename(key_meta.filename_full)
    print "[%s] uploaded" % key_full.key

def cleanUp(key_meta):
    os.remove(key_meta.filename + ".NEF")
    os.remove(key_meta.filename_tiny)
    os.remove(key_meta.filename_prev)
    os.remove(key_meta.filename_full)

start = time.time()
file_count = 0
for key in content:
    print "Getting raw from S3"
    key_meta = getRawFromS3(key)

    if(key_meta):
        print "Raw [%s] retrieved" % key_meta.filename

        print "Producing bitmaps"
        produceJPGs(key_meta)
        print "Bitmaps produced"

        print "Uploading bitmaps to S3"
        uploadToS3(key_meta)
        print "All bitmaps uploaded to S3"

        print "Cleaning up"
        cleanUp(key_meta)

        file_count = file_count + 1
        if(file_count > 2):
            break
    else:
        print "Non-RAW found, skipping"
end = time.time()

time_delta = end - start
print "Completed for [%s] files in [%s] seconds" % (file_count, time_delta)