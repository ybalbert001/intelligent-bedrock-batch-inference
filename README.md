# Intelligent Bedrock Batch Inference

This implementation provides a scalable solution for running batch inference using Amazon Bedrock through AWS Glue. The solution processes JSONL files from S3 and controls the rate of API calls to Bedrock.

## Usage

1. Open AWS Console, navigate to Cloudformation, create stack by **template.yaml**

2. Run the Glue Job
```bash
aws glue start-job-run \
    --job-name intelligent-bedrock-batch-inference \
    --arguments '{
        "--input_path": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-input/translation_eval/haiku3-novaLite-8.jsonl",
        "--output_path": "s3://translation-quality-check-model-sft-20241203/amazon-review-product-meta-data/batch-inference-output/translation_eval/c35-v2/haiku3-novaLite-8.jsonl.out",
        "--model_id": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "--rpm": "50",
        "--max_worker" : "10",
        "--ak" : "{ak}",
        "--sk" : "{sk}",
        "--region" : "{region}"
    }'
```

### Parameters

- `input_path`: S3 path to input JSONL files
- `output_path`: S3 path for output results
- `model_id`: Bedrock model ID to use
- `rpm`: Requests per minute limit (adjust based on your quota)