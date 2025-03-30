# Intelligent Bedrock Batch Inference

This implementation provides a scalable solution for running batch inference using Amazon Bedrock through AWS Glue. The solution processes JSONL files from S3 and controls the rate of API calls to Bedrock.

## Usage

1. Open AWS Console, navigate to Cloudformation, create stack by **template.yaml**

2. Run Batch Inference(AWS Glue)

- [Option1] Web UI
   ```bash
   ## run locally
   pip install -r requirements.txt
   streamlit run app.py
   
   ## Docker 
   
   ```

- [Option2] CLI method

    - InvokeModel BatchInference (For Single s3_uri)
    ```bash
    aws glue start-job-run \
        --job-name intelligent-bedrock-batch-inference \
        --arguments '{
            "--input_s3_uri_list": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Appliances_0.jsonl",
            "--output_s3_uri": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-output/c35-v2/zh-cn/",
            "--model_id": "anthropic.claude-3-haiku-20240307-v1:0",
            "--rpm": "50",
            "--max_worker" : "10",
            "--ak" : "{ak}",
            "--sk" : "{sk}",
            "--region" : "{region}"
        }'
    ```

    - InvokeModel BatchInference (For multiple s3_uri)
    ```bash
    aws glue start-job-run \
        --job-name intelligent-bedrock-batch-inference \
        --arguments '{
            "--input_s3_uri_list": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Appliances_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Arts_Crafts_and_Sewing_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Books_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Electronics_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Industrial_and_Scientific_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Kindle_Store_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Software_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Toys_and_Games_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Unknown_0.jsonl,s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/claude/zh-cn/meta_Video_Games_0.jsonl",
            "--output_s3_uri": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-output/haiku3/zh-cn/",
            "--model_id": "anthropic.claude-3-haiku-20240307-v1:0",
            "--rpm": "800",
            "--max_worker" : "10",
            "--ak" : "{ak}",
            "--sk" : "{sk}",
            "--region" : "us-west-2"
        }'
    ```

    - InvokeDifyWorkflow BatchInference (For single s3_uri)
    ```bash
    aws glue start-job-run \
        --job-name intelligent-bedrock-batch-inference \
        --arguments '{
            "--input_s3_uri_list": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/dify_input_1.jsonl",
            "--output_s3_uri": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-output/dify_output/",
            "--dify_workflow_url": "{dify_workflow_url}",
            "--dify_workflow_key": "{dify_workflow_key",
            "--rpm": "50",
            "--max_worker" : "10",
        }'
    ```

### Parameters

- `input_s3_uri_list`: S3 path list which provide input JSONL files, splited by ,
- `output_s3_uri`: S3 path for output results
- `model_id`: Bedrock model ID to use
- `rpm`: Requests per minute limit (adjust based on your quota)
