
resource "aws_glue_job" "foregon_silver" {
    name = "foregon_silver"
    role_arn = var.glue_role_arn

    command {
        script_location = "${var.scripts_full_path}foregon-silver.py"
        name = "glueetl"

        python_version = "3"
    }

    glue_version = "3.0"

    worker_type = "G.1X"
    number_of_workers = 2

    depends_on = [aws_s3_bucket_object.foregon_silver]

}


resource "aws_glue_job" "foregon_user_products" {
    name = "foregon_user_products"
    role_arn = var.glue_role_arn

    command {
        script_location = "${var.scripts_full_path}foregon-user-products.py"
        name = "glueetl"

        python_version = "3"
    }

    glue_version = "3.0"

    worker_type = "G.1X"
    number_of_workers = 2

    depends_on = [aws_s3_bucket_object.foregon_user_products]

}

resource "aws_glue_job" "foregon_user_scores" {
    name = "foregon_user_scores"
    role_arn = var.glue_role_arn

    command {
        script_location = "${var.scripts_full_path}foregon-user-scores.py"
        name = "glueetl"

        python_version = "3"
    }

    glue_version = "3.0"

    worker_type = "G.1X"
    number_of_workers = 2

    depends_on = [aws_s3_bucket_object.foregon_user_scores]

}