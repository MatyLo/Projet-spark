resource "aws_glue_job" "example" {
  name     = "example"
  role_arn = aws_iam_role.glue.arn  # On lie le job au rôle IAM qu’on vient de créer

  command {
    script_location = "s3://${aws_s3_bucket.my_bucket.bucket}/spark-jobs/exo2_glue_job.py"
    python_version  = "3"
  }

  glue_version = "3.0"
  number_of_workers = 2
  worker_type = "Standard"
  
  default_arguments = {
    "--additional-python-modules"       = "s3://${aws_s3_bucket.my_bucket.bucket}/wheel/spark-handson.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "VALUE_1"
    "--PARAM_2"                         = "VALUE_2"
  }
}