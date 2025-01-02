#!/usr/bin/env python3
import sys
import json
import boto3
import argparse
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

from functools import wraps
from math import floor
import time
import sys
import threading
from awsglue.utils import getResolvedOptions

# Use monotonic time if available, otherwise fall back to the system clock.
now = time.monotonic if hasattr(time, 'monotonic') else time.time

class RateLimitException(Exception):
    '''
    Rate limit exception class.
    '''
    def __init__(self, message, period_remaining):
        '''
        Custom exception raise when the number of function invocations exceeds
        that imposed by a rate limit. Additionally the exception is aware of
        the remaining time period after which the rate limit is reset.

        :param string message: Custom exception message.
        :param float period_remaining: The time remaining until the rate limit is reset.
        '''
        super(RateLimitException, self).__init__(message)
        self.period_remaining = period_remaining

class RateLimitDecorator(object):
    '''
    Rate limit decorator class.
    '''
    def __init__(self, calls=15, period=900, clock=now, raise_on_limit=True):
        '''
        Instantiate a RateLimitDecorator with some sensible defaults. By
        default the Twitter rate limiting window is respected (15 calls every
        15 minutes).

        :param int calls: Maximum function invocations allowed within a time period. Must be a number greater than 0.
        :param float period: An upper bound time period (in seconds) before the rate limit resets. Must be a number greater than 0.
        :param function clock: An optional function retuning the current time. This is used primarily for testing.
        :param bool raise_on_limit: A boolean allowing the caller to avoiding rasing an exception.
        '''
        self.clamped_calls = max(1, min(sys.maxsize, floor(calls)))
        self.period = period
        self.clock = clock
        self.raise_on_limit = raise_on_limit

        # Initialise the decorator state.
        self.last_reset = clock()
        self.num_calls = 0

        # Add thread safety.
        self.lock = threading.RLock()

    def __call__(self, func):
        '''
        Return a wrapped function that prevents further function invocations if
        previously called within a specified period of time.

        :param function func: The function to decorate.
        :return: Decorated function.
        :rtype: function
        '''
        @wraps(func)
        def wrapper(*args, **kargs):
            '''
            Extend the behaviour of the decoated function, forwarding function
            invocations previously called no sooner than a specified period of
            time. The decorator will raise an exception if the function cannot
            be called so the caller may implement a retry strategy such as an
            exponential backoff.

            :param args: non-keyword variable length argument list to the decorated function.
            :param kargs: keyworded variable length argument list to the decorated function.
            :raises: RateLimitException
            '''
            with self.lock:
                period_remaining = self.__period_remaining()

                # If the time window has elapsed then reset.
                if period_remaining <= 0:
                    self.num_calls = 0
                    self.last_reset = self.clock()

                # Increase the number of attempts to call the function.
                self.num_calls += 1

                # If the number of attempts to call the function exceeds the
                # maximum then raise an exception.
                if self.num_calls > self.clamped_calls:
                    if self.raise_on_limit:
                        raise RateLimitException('too many calls', period_remaining)
                    return

            return func(*args, **kargs)
        return wrapper

    def __period_remaining(self):
        '''
        Return the period remaining for the current rate limit window.

        :return: The remaing period.
        :rtype: float
        '''
        elapsed = self.clock() - self.last_reset
        return self.period - elapsed

limits = RateLimitDecorator

def sleep_and_retry(func):
    '''
    Return a wrapped function that rescues rate limit exceptions, sleeping the
    current thread until rate limit resets.

    :param function func: The function to decorate.
    :return: Decorated function.
    :rtype: function
    '''
    @wraps(func)
    def wrapper(*args, **kargs):
        '''
        Call the rate limited function. If the function raises a rate limit
        exception sleep for the remaing time period and retry the function.

        :param args: non-keyword variable length argument list to the decorated function.
        :param kargs: keyworded variable length argument list to the decorated function.
        '''
        while True:
            try:
                return func(*args, **kargs)
            except RateLimitException as exception:
                time.sleep(exception.period_remaining)
    return wrapper

# record schema
# {"recordId": "meta_All_Beauty_0_0", "modelInput": {"anthropic_version": "bedrock-2023-05-31", "max_tokens": 2048, "stop_sequences": ["</translation>"], "messages": [{"role": "user", "content": [{"type": "text", "text": "你是一位翻译专家，擅长翻译商品title。请精准的把<src>中的商品Title翻译为ru-ru, 输出到<translation> xml tag中。\n<src>Yes to Tomatoes Detoxifying Charcoal Cleanser (Pack of 2) with Charcoal Powder, Tomato Fruit Extract, and Gingko Biloba Leaf Extract, 5 fl. oz.</src>\n"}]}, {"role": "assistant", "content": [{"type": "text", "text": "<translation>"}]}]}}

# inference output schema
# {"modelInput":{"anthropic_version":"bedrock-2023-05-31","max_tokens":2048,"stop_sequences":["</translation>"],"messages":[{"role":"user","content":[{"type":"text","text":"你是一位翻译专家，擅长翻译商品title。请精准的把<src>中的商品Title翻译为ru-ru, 输出到<translation> xml tag中。\n<src>HANSGO Egg Holder for Refrigerator, Deviled Eggs Dispenser Egg Storage Stackable Plastic Egg Containers Hold ups to 10 Eggs</src>\n"}]},{"role":"assistant","content":[{"type":"text","text":"<translation>"}]}]},"modelOutput":{"id":"msg_bdrk_018FaiigbPq6PehfhWM2GMBv","type":"message","role":"assistant","model":"claude-3-haiku-20240307","content":[{"type":"text","text":"HANSGO Держатель для яиц для холодильника, диспенсер для яиц в горшочках, стопки для хранения яиц, пластиковые контейнеры для яиц, вмещающие до 10 яиц"}],"stop_reason":"stop_sequence","stop_sequence":"</translation>","usage":{"input_tokens":111,"output_tokens":73}},"recordId":"meta_Appliances_0_0"}

## InvokeModel Request Body
# {
#     "anthropic_version": "bedrock-2023-05-31", 
#     "anthropic_beta": ["computer-use-2024-10-22"] 
#     "max_tokens": int,
#     "system": string,    
#     "messages": [
#         {
#             "role": string,
#             "content": [
#                 { "type": "image", "source": { "type": "base64", "media_type": "image/jpeg", "data": "content image bytes" } },
#                 { "type": "text", "text": "content text" }
#       ]
#         }
#     ],
#     "temperature": float,
#     "top_p": float,
#     "top_k": int,
#     "tools": [
#         {
#                 "type": "custom",
#                 "name": string,
#                 "description": string,
#                 "input_schema": json
            
#         },
#         { 
#             "type": "computer_20241022",  
#             "name": "computer", 
#             "display_height_px": int,
#             "display_width_px": int,
#             "display_number": 0 int
#         },
#         { 
#             "type": "bash_20241022", 
#             "name": "bash"
#         },
#         { 
#             "type": "text_editor_20241022",
#             "name": "str_replace_editor"
#         }
        
#     ],
#     "tool_choice": {
#         "type" :  string,
#         "name" : string,
#     },
    

 
#     "stop_sequences": [string]
# }              


class BedrockBatchInference:
    # Class-level rate limiter to ensure it's shared across all instances and threads
    _rate_limiter = None
    _rate_limiter_lock = threading.Lock()
    _call_count = 0
    _last_call_time = time.time()

    def __init__(self, model_id: str, rpm: int, region:str, ak:str, sk:str):
        """Initialize the batch inference processor"""
        print(f"ak: {ak}")
        print(f"sk: {sk}")
        print(f"region: {region}")
        self.bedrock = boto3.client('bedrock-runtime', region_name=region, aws_access_key_id=ak, aws_secret_access_key=sk)
        self.model_id = model_id
        self.rpm = rpm
        
        # Initialize or get the shared rate limiter
        with self._rate_limiter_lock:
            if BedrockBatchInference._rate_limiter is None:
                BedrockBatchInference._rate_limiter = limits(calls=rpm, period=60)
                print(f"Initialized rate limiter with {rpm} calls per minute")
        
        # Create rate-limited invoke function for this instance
        self._rate_limited_invoke = sleep_and_retry(BedrockBatchInference._rate_limiter(self._invoke_model))

    def invoke_model_with_rate_limit(self, record: Dict) -> Dict:
        """Process a single record through Bedrock's model with rate limiting"""
        with self._rate_limiter_lock:
            current_time = time.time()
            BedrockBatchInference._call_count += 1
            
            # If this is the first call in a new RPM cycle
            if (BedrockBatchInference._call_count - 1) % self.rpm == 0:
                elapsed = current_time - BedrockBatchInference._last_call_time
                print(f"\nStarting new RPM cycle at call {BedrockBatchInference._call_count}")
                print(f"Time since last cycle: {elapsed:.2f}s")
                
                # If we're starting a new cycle too soon, sleep until the full minute has passed
                if elapsed < 60:
                    sleep_time = 60 - elapsed
                    print(f"Rate limit reached, sleeping for {sleep_time:.2f}s to start new cycle")
                    time.sleep(sleep_time)
                
                # Update times after potential sleep
                current_time = time.time()
                BedrockBatchInference._last_call_time = current_time
                elapsed = 0  # Reset elapsed time since this is the start of a new cycle
            else:
                # Calculate elapsed time since cycle start
                elapsed = current_time - BedrockBatchInference._last_call_time
            
            # Log the call details
            print(f"Call {BedrockBatchInference._call_count} - Time since cycle start: {elapsed:.2f}s")
        
        return self._rate_limited_invoke(record)

    def _invoke_model(self, record: Dict) -> Dict:
        """Internal method to invoke the Bedrock model with the given record"""
        try:
            # Extract the model input from the record
            model_input = record.get('modelInput', {})
            if not model_input:
                raise ValueError("No model input found in the record.")

            # Log request details
            print(f"Processing record {record.get('recordId', 'unknown')} at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Current thread: {threading.current_thread().name}")
            with self._rate_limiter.lock:
                period_remaining = self._rate_limiter.period - (self._rate_limiter.clock() - self._rate_limiter.last_reset)
                print(f"Rate limiter state - calls: {self._rate_limiter.num_calls}, period remaining: {period_remaining:.2f}s")
            
            # Prepare and send request
            payload = json.dumps(model_input)
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=payload
            )
            
            # Parse and log response
            response_body = json.loads(response.get('body').read())
            print(f"Successfully processed record {record.get('recordId', 'unknown')}")
            # Construct the output following the inference output schema
            return {
                'modelInput': model_input,
                'modelOutput': response_body,
                'recordId': record.get('recordId', '')
            }
        except Exception as e:
            print(f"exception: {e}")
            return {
                'modelInput': model_input,
                'modelOutput': {
                    'error': str(e)
                },
                'recordId': record.get('recordId', '')
            }

    def process_batch(self, records: List[Dict], max_workers: int) -> List[Dict]:
        """Process a batch of records using thread pool"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            return list(executor.map(self.invoke_model_with_rate_limit, records))

def read_jsonl(s3_path: str) -> List[Dict]:
    """Read JSONL data from S3"""
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    response = s3.get_object(Bucket=bucket, Key=key)
    result = []
    for line_number, line in enumerate(response['Body'].read().decode('utf-8').splitlines(), 1):
        try:
            parsed_line = json.loads(line)
            result.append(parsed_line)
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse JSON at line {line_number}. Error: {str(e)}")
            print(f"Problematic line: {line}")
            continue
    return result

def write_jsonl(data: List[Dict], s3_path: str):
    """Write JSONL data to S3"""
    s3 = boto3.client('s3')
    bucket, key = s3_path.replace('s3://', '').split('/', 1)
    content = '\n'.join(json.dumps(record, ensure_ascii=False) for record in data)
    s3.put_object(Bucket=bucket, Key=key, Body=content.encode('utf-8'))

def main():
    """Main execution function"""
    # parser = argparse.ArgumentParser(description='Process CSV files for translation')
    # parser.add_argument('--input_path', type=str, default='zh-cn',
    #                   help='Target language for translation (default: zh-cn)')
    # parser.add_argument('--output_path', type=str, default='claude',
    #                   help='Target model : claude')
    # parser.add_argument('--model_id', type=str, default='claude',
    #                 help='Target model : claude')
    # parser.add_argument('--rpm', type=int, default=20,
    #                 help='Target model : claude')
    # parser.add_argument('--max_worker', type=int, default=20,
    #                 help='max_worker')
    # parser.add_argument('--ak', type=str, default='',
    #                 help='access key for aws')
    # parser.add_argument('--sk', type=str, default='',
    #                 help='secret key for aws')
    # parser.add_argument('--region', type=str, default='',
    #                 help='region')
    # args = parser.parse_args()

    # input_path = args.input_path
    # output_path = args.output_path
    # model_id = args.model_id
    # rpm = args.rpm
    # max_worker = args.max_worker
    # ak = args.ak
    # sk = args.sk
    # region = args.region

    args = getResolvedOptions(sys.argv, ['input_s3_uri_list', 'output_s3_uri', 'model_id', 'rpm', 'max_worker', 'ak', 'sk', 'region'])
    input_s3_uri_list = args['input_s3_uri_list'].split(',')  # Parse JSON string to list
    output_path = args['output_s3_uri']
    model_id = args['model_id']
    rpm = int(args['rpm'])
    max_worker = int(args.get('max_worker', 10))
    ak = args['ak']
    sk = args['sk']
    region = args['region']

    # Initialize processor
    processor = BedrockBatchInference(model_id, rpm, region, ak, sk)
    
    try:
        print(f"Processing {len(input_s3_uri_list)} input files")
        
        # Process each input file
        for input_uri in input_s3_uri_list:
            if not input_uri.endswith('.jsonl'):
                print(f"Warning: Skipping {input_uri} as it's not a JSONL file")
                continue
                
            # Generate output path by appending .out to the input filename
            file_name = input_uri.split('/')[-1]
            output_bucket, output_prefix = output_path.replace('s3://', '').split('/', 1)
            if not output_prefix.endswith('/'):
                output_prefix += '/'
            current_output_path = f"s3://{output_bucket}/{output_prefix}{file_name}.out"
            
            print(f"\nProcessing file: {input_uri}")
            print(f"Output will be written to: {current_output_path}")
            
            all_records = read_jsonl(input_uri)
            records = all_records
            results = processor.process_batch(records, max_worker)
            write_jsonl(results, current_output_path)
        
        print("Processing completed successfully")
        
    except Exception as e:
        print(f"Error during processing: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
