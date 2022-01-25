resource "aws_s3_bucket_object" "foregon_silver" {
    bucket = var.bucket_scripts
    key = "foregon-silver.py"
    source = "../ETL/etl_silver.py"

    etag = filemd5("../ETL/etl_silver.py")
}

resource "aws_s3_bucket_object" "foregon_user_products" {
    bucket = var.bucket_scripts
    key = "foregon-user-products.py"
    source = "../ETL/etl_user_products_gold.py"

    etag = filemd5("../ETL/etl_user_products_gold.py")
}

resource "aws_s3_bucket_object" "foregon_user_scores" {
    bucket = var.bucket_scripts
    key = "foregon-user-scores.py"
    source = "../ETL/etl_user_scores_gold.py"

    etag = filemd5("../ETL/etl_user_scores_gold.py")
}
