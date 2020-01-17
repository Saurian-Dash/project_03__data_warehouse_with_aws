REDSHIFT_FULL_ACCESS = {
    "name": "AmazonRedshiftFullAccess",
    "arn": "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess",
}

S3_READ_ACCESS = {
    "name": "AmazonS3ReadAccess",
    "arn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
}

REDSHIFT_TRUST_RELATIONSHIP = {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
