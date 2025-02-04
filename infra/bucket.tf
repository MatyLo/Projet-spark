resource "aws_s3_bucket" "my_bucket" {
  bucket = "my-bucket-hu-angel"

  tags = local.tags
}