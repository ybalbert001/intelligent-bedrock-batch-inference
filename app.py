import streamlit as st
import subprocess
import json
import boto3
import pandas as pd
import os
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="Intelligent Bedrock Batch Inference",
    page_icon="🤖",
    layout="wide"
)

# Initialize session state variables if they don't exist
if 'jobs' not in st.session_state:
    st.session_state.jobs = []
if 'job_details' not in st.session_state:
    st.session_state.job_details = {}
if 'job_type' not in st.session_state:
    st.session_state.job_type = "InvokeModel"

# Get AWS credentials from boto3
try:
    session = boto3.Session()
    credentials = session.get_credentials()
    aws_region = session.region_name or "us-west-2"  # Default to us-west-2 if not set
    st.success(f"AWS credentials loaded successfully. Region: {aws_region}")
except Exception as e:
    st.error(f"Error loading AWS credentials: {e}")
    credentials = None
    aws_region = "us-west-2"

# Function to run AWS CLI commands
def run_aws_command(command):
    try:
        env = os.environ.copy()
        # Use the automatically loaded AWS region
        env["AWS_DEFAULT_REGION"] = aws_region
            
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True,
            env=env
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        st.error(f"Error executing AWS command: {e}")
        st.error(f"Command output: {e.stdout}")
        st.error(f"Command error: {e.stderr}")
        return None

# Function to start a batch inference job
def start_job(job_args):
    args_json = json.dumps(job_args)
    command = f"aws glue start-job-run --job-name intelligent-bedrock-batch-inference --arguments '{args_json}'"
    result = run_aws_command(command)
    
    if result and 'JobRunId' in result:
        job_id = result['JobRunId']
        job_name = 'intelligent-bedrock-batch-inference'
        
        # Add job to session state
        job_info = {
            'job_id': job_id,
            'job_name': job_name,
            'start_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'status': 'STARTING',
            'args': job_args
        }
        
        st.session_state.jobs.append(job_info)
        st.session_state.job_details[job_id] = {
            'info': job_info,
            'output_files': []
        }
        
        return job_id
    return None

# Function to get job status
def get_job_status(job_name, job_id):
    command = f"aws glue get-job-run --job-name {job_name} --run-id {job_id}"
    result = run_aws_command(command)
    
    if result and 'JobRun' in result:
        return {
            'status': result['JobRun']['JobRunState'],
            'start_time': result['JobRun'].get('StartedOn', ''),
            'end_time': result['JobRun'].get('CompletedOn', ''),
            'error_message': result['JobRun'].get('ErrorMessage', ''),
            'execution_time': result['JobRun'].get('ExecutionTime', 0)
        }
    return {'status': 'UNKNOWN'}

# Removed get_job_logs function

# Function to list S3 output files
def list_s3_output_files(output_uri):
    try:
        # Parse S3 URI
        if not output_uri.startswith('s3://'):
            return []
        
        parts = output_uri.replace('s3://', '').split('/', 1)
        if len(parts) < 2:
            return []
            
        bucket = parts[0]
        prefix = parts[1]
        
        # Ensure prefix ends with /
        if not prefix.endswith('/'):
            prefix += '/'
            
        command = f"aws s3 ls s3://{bucket}/{prefix} --recursive"
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            env=os.environ.copy()
        )
        
        files = []
        for line in result.stdout.splitlines():
            if line.strip():
                parts = line.split()
                if len(parts) >= 4:
                    size = parts[2]
                    file_path = ' '.join(parts[3:])
                    files.append({
                        'path': f"s3://{bucket}/{file_path}",
                        'size': size
                    })
        
        return files
    except Exception as e:
        st.error(f"Error listing S3 files: {e}")
        return []

# Removed refresh_job_statuses function

# Main app layout
st.title("🤖 Intelligent Bedrock Batch Inference")

# No sidebar for AWS configuration - using local credentials automatically

# Create tabs
tab1, tab2 = st.tabs(["Start Job", "Job Tracker"])

# Tab 1: Start Job
with tab1:
    st.header("Start Batch Inference Job")
    
    # Job type selection outside the form
    job_type = st.selectbox(
        "Job Type",
        ["InvokeModel", "InvokeDifyWorkflow"],
        index=0 if st.session_state.job_type == "InvokeModel" else 1
    )
    
    # Update session state job type
    if job_type != st.session_state.job_type:
        st.session_state.job_type = job_type
        st.rerun()
    
    # Create form for job parameters
    with st.form("job_form"):
        # Common parameters
        input_s3_uri_list = st.text_area(
            "Input S3 URI List (comma-separated)",
            placeholder="s3://bucket/path/file1.jsonl,s3://bucket/path/file2.jsonl"
        )
        
        output_s3_uri = st.text_input(
            "Output S3 URI",
            placeholder="s3://bucket/output/path/"
        )
        
        rpm = st.number_input("Requests Per Minute (RPM)", min_value=1, value=50)
        max_worker = st.number_input("Max Workers", min_value=1, value=10)
        
        # Job-specific parameters
        if st.session_state.job_type == "InvokeModel":
            model_id = st.text_input(
                "Model ID",
                placeholder="anthropic.claude-3-haiku-20240307-v1:0"
            )
            
            # Show info about using local credentials
            st.info(f"Using AWS credentials from local configuration. Region: {aws_region}")
            
            # Still allow overriding credentials if needed
            with st.expander("Override AWS Credentials (optional)"):
                ak = st.text_input("AWS Access Key", type="password")
                sk = st.text_input("AWS Secret Key", type="password")
                region = st.text_input("AWS Region", value=aws_region)
            
        else:  # InvokeDifyWorkflow
            dify_workflow_url = st.text_input(
                "Dify Workflow URL",
                placeholder="https://api.dify.ai/v1/workflows/123456/run"
            )
            
            dify_workflow_key = st.text_input(
                "Dify Workflow Key",
                type="password"
            )
        
        submitted = st.form_submit_button("Start Job")
        
        if submitted:
            # Validate inputs
            if not input_s3_uri_list:
                st.error("Input S3 URI List is required")
            elif not output_s3_uri:
                st.error("Output S3 URI is required")
            else:
                # Prepare job arguments
                job_args = {
                    "--input_s3_uri_list": input_s3_uri_list,
                    "--output_s3_uri": output_s3_uri,
                    "--rpm": str(rpm),
                    "--max_worker": str(max_worker)
                }
                
                if st.session_state.job_type == "InvokeModel":
                    if not model_id:
                        st.error("Model ID is required")
                        st.stop()
                        
                    job_args["--model_id"] = model_id
                    
                    if ak:
                        job_args["--ak"] = ak or credentials.access_key
                    if sk:
                        job_args["--sk"] = sk or credentials.secret_key
                    if region:
                        job_args["--region"] = region

                    job_args["--dify_workflow_url"] = '?'
                    job_args["--dify_workflow_key"] = '?'
                    
                    # For InvokeModel, we don't include dify_workflow_url and dify_workflow_key
                        
                else:  # InvokeDifyWorkflow
                    if not dify_workflow_url:
                        st.error("Dify Workflow URL is required")
                        st.stop()
                    if not dify_workflow_key:
                        st.error("Dify Workflow Key is required")
                        st.stop()
                        
                    job_args["--dify_workflow_url"] = dify_workflow_url
                    job_args["--dify_workflow_key"] = dify_workflow_key
                    job_args["--sk"] = '?'
                    job_args["--ak"] = '?'
                    job_args["--region"] = '?'
                    job_args["--model_id"] = '?'
                
                # Start the job
                with st.spinner("Starting job..."):
                    job_id = start_job(job_args)
                    
                if job_id:
                    st.success(f"Job started successfully! Job ID: {job_id}")
                else:
                    st.error("Failed to start job. Check the logs for details.")

# Tab 2: Job Tracker
with tab2:
    st.header("Job Tracker")
    
    # Refresh button
    if st.button("🔄 Refresh"):
        for job in st.session_state.jobs:
            job_id = job['job_id']
            job_name = job['job_name']
            
            # Update job status
            status_info = get_job_status(job_name, job_id)
            job['status'] = status_info['status']
            
            # Update job details
            if job_id in st.session_state.job_details:
                st.session_state.job_details[job_id]['info']['status'] = status_info['status']
                
                # If job completed, get output files
                if status_info['status'] in ['SUCCEEDED', 'FAILED', 'TIMEOUT', 'STOPPED']:
                    output_uri = job['args'].get('--output_s3_uri', '')
                    if output_uri:
                        files = list_s3_output_files(output_uri)
                        st.session_state.job_details[job_id]['output_files'] = files
    
    # Display jobs table
    if not st.session_state.jobs:
        st.info("No jobs have been started yet. Use the 'Start Job' tab to begin.")
    else:
        # Create DataFrame for jobs
        jobs_data = []
        for job in st.session_state.jobs:
            jobs_data.append({
                'Job ID': job['job_id'],
                'Start Time': job['start_time'],
                'Status': job['status'],
                'Job Type': 'InvokeModel' if '--model_id' in job['args'] else 'InvokeDifyWorkflow'
            })
        
        jobs_df = pd.DataFrame(jobs_data)
        st.dataframe(jobs_df, use_container_width=True)
        
        # Job details expander
        st.subheader("Job Details")
        for job in st.session_state.jobs:
            job_id = job['job_id']
            
            with st.expander(f"Job {job_id} - Status: {job['status']}"):
                # Job information
                st.write("**Job Information**")
                st.json(job['args'])
                
                # Status information
                st.write("**Status Information**")
                status_info = get_job_status(job['job_name'], job_id)
                st.json(status_info)
                
                # Output files
                st.write("**Output Files**")
                if job_id in st.session_state.job_details and st.session_state.job_details[job_id].get('output_files'):
                    files = st.session_state.job_details[job_id]['output_files']
                    if files:
                        files_df = pd.DataFrame(files)
                        st.dataframe(files_df, use_container_width=True)
                    else:
                        st.info("No output files found yet.")
                else:
                    output_uri = job['args'].get('--output_s3_uri', '')
                    if output_uri and job['status'] in ['SUCCEEDED', 'FAILED', 'TIMEOUT', 'STOPPED']:
                        files = list_s3_output_files(output_uri)
                        if job_id in st.session_state.job_details:
                            st.session_state.job_details[job_id]['output_files'] = files
                        
                        if files:
                            files_df = pd.DataFrame(files)
                            st.dataframe(files_df, use_container_width=True)
                        else:
                            st.info("No output files found.")
                    else:
                        st.info("Output files will be available when the job completes.")

# Footer
st.markdown("---")
st.markdown("### About")
st.markdown("""
This application provides a user interface for running batch inference jobs using Amazon Bedrock through AWS Glue.
It allows you to start jobs, track their status, and view results.

For more information, refer to the [GitHub repository](https://github.com/yourusername/intelligent-bedrock-batch-inference).
""")
