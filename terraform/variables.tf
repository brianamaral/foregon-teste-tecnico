variable "bucket_raw" {
    type = string
    default = "foregon-raw"
}

variable "bucket_scripts" {
    type = string
    default = "glue-jobs-01"
}
variable "scripts_path" {
    type = string
    default = "glue/"
}

variable "scripts_full_path" {
    type = string
    default = "s3://glue-jobs-01/"
}

variable "glue_role_arn" {
    type = string
    default = "arn:aws:iam::669032577577:role/aws_glue_blueprint"
}