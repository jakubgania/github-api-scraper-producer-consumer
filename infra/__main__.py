"""An AWS Python Pulumi program"""

import pulumi
import pulumi_aws as aws
import json

# Create an AWS resource (S3 Bucket)
# bucket = s3.Bucket('my-bucket')

# Export the name of the bucket
# pulumi.export('bucket_name', bucket.id)

bucket = aws.s3.BucketV2(
  "dashboard-bucket",
  bucket_prefix="analytics-dashboard-",
  force_destroy="text/html",
)

index_file = aws.s3.BucketObject(
  "index-html",
  bucket=bucket.id,
  source=pulumi.FileAsset("path to file"),
  content_type="text/html",
)

oac = aws.cloudfront.OriginAccessControl(
  "oac",
  description="OAC for S3 bucket",
  origin_access_control_origin_type="s3",
  signing_behavior="always",
  signing_protocol="sigv4",
)

distribution = aws.cloudfront.Distribution(
  "cdn",
  enabled=True,
  origins=[aws.cloudfront.DistributionOriginArgs(
    domain_name=bucket.bucket_regional_domain_name,
    origin_id=bucket.arn,
    origin_access_control_id=oac.id,
  )],
  default_root_object="index.html",
  default_cache_behavior=aws.cloudfront.DistributionDefaultCacheBehaviorArgs(
    target_origin_id=bucket.arn,
    viewer_protocol_policy="redirect-to-https",
    allowed_methods=["GET", "HEAD"],
    cached_methods=["GET", "HEAD"],
    forwarded_values=aws.cloudfront.DistributionDefaultCacheBehaviorForwardedValuesArgs(
      query_string=False,
      cookies=aws.cloudfront.DistributionDefaultCacheBehaviorForwardedValuesCookiesArgs(
        forward="none",
      ),
    ),
    default_ttl=3600,
    min_ttl=0,
    max_ttl=86400,
  ),
  restrictions=aws.cloudfront.DistributionRestrictionsArgs(
    geo_restriction=aws.cloudfront.DistributionRestrictionsGeoRestrictionArgsDict(
      restriction_type="none",
    ),
  ),
  viewer_certificate=aws.cloudfront.DistributionViewerCertificateArgs(
    cloudfront_default_certificate=True,
  )
)

bucket_policy = aws.s3.BucketPolicy(
  "bucket-policy",
  bucket=bucket.id,
  policy=pulumi.Output.all(bucket.arn, distribution.arn).apply(
    lambda args: json.dumps({
      "Version": "2012-10-17",
      "Statement": [{
        "Sid": "AllowCloudFrontOAC",
        "Effect": "Allow",
        "Principal": {"Service": "cloudfront.amazonaws.com"},
        "Action": "s3:GetObject",
        "Resource": f"{args[0]}/*",
        "Condition": {
          "StringEquals": {
            "AWS:SourceArn": args[1]
          }
        }
      }]
    })
  )
)

pulumi.export("bucket_name", bucket.id)
pulumi.export("cloudfront_url", distribution.domain_name)
