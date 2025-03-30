# Intelligent Bedrock Batch Inference

本项目通过AWS Glue服务实现了可扩展的运行批量推理的方案。它处理S3上的JSONL文件作为输入，把推理结果输出到S3的指定路径中。该方案可以通过设定rpm来控制推理的速率和并发。

## 使用方法

1. 打开AWS Console, 选择Cloudformation服务, 用文件**template.yaml**模版一键部署。

2. 运行Glue job进行批量推理

- [Option1] Web UI方式
   ```bash
   ## 本地安装运行
   pip install -r requirements.txt
   streamlit run app.py
   
   ## Docker运行
   
   ```
   
- [Option2] CLI方式

   - 模型批量推理 (单S3_URI输入)
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
     
   - 模型批量推理 (多S3_URI输入)
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
   - Dify工作流批量推理(单S3_URI输入)
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

### 参数

- `input_s3_uri_list`: s3输入列表，用','分隔
- `output_s3_uri`: s3输出路径
- `model_id`: 采样的Bedrock model ID
- `rpm`: 每分钟请求数限制
