import json
import subprocess
import shlex
import boto3
import os


def lambda_handler(event, context):

    s3_client = boto3.client('s3')

    s3_source_bucket = event['Records'][0]['s3']['bucket']['name']
    s3_source_key = event['Records'][0]['s3']['object']['key']

    s3_source_basename = os.path.splitext(os.path.basename(s3_source_key))[0]
    destination_bucket = "customer-slates"
    destination_bucketfile_name = os.path.dirname(s3_source_key) + "/" + s3_source_basename + ".ts"

    l_input = "/tmp/" + os.path.basename(s3_source_key)
    l_output = "/tmp/"+ s3_source_basename + ".ts"

    # Download the input video file from S3
    s3_client.download_file(s3_source_bucket,s3_source_key,l_input)

    # Use ffprobe to check if the input video has an audio track
    ffprobe_cmd = "/opt/bin/ffprobe -i \"" + l_input + "\" -show_streams -select_streams a -loglevel error"
    p = subprocess.Popen(ffprobe_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = p.communicate()
    if err:
        print("Error running ffprobe: {}".format(err))
        return {
            'statusCode': 400,
            'body': json.dumps("Error running ffprobe: {}".format(err))
        }
    if not out:
        # Add a silent audio track using ffmpeg
        ffmpeg_cmd = "/opt/bin/ffmpeg -y -i \"" + l_input + "\" -f lavfi -i anullsrc=channel_layout=stereo:sample_rate=44100 -c:v libx264 -c:a aac -tune zerolatency -f mpegts -shortest  " + l_output
    else:
        # Copy the video and audio streams using ffmpeg
        ffmpeg_cmd = "/opt/bin/ffmpeg -y -i \"" + l_input + "\" -vcodec libx264 -crf 23 -f mpegts " + l_output

    # Run the ffmpeg command
    print(ffmpeg_cmd)
    p1 = subprocess.call(ffmpeg_cmd, shell=True)

    response = "Failed"
    if p1 == 0:
        # Upload the output file to S3
        response = s3_client.upload_file(l_output, Bucket=destination_bucket, Key=destination_bucketfile_name, ExtraArgs={'ACL':'public-read'})
    else:
        print("Failed Process "+str(p1))

    # Clean up the temporary input and output files
    os.remove(l_input)
    os.remove(l_output)

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
