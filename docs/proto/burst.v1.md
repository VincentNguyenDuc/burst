# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [burst/v1/control.proto](#burst_v1_control-proto)
    - [ControllerRpc](#burst-v1-ControllerRpc)
  
- [burst/v1/job.proto](#burst_v1_job-proto)
    - [AssignedJob](#burst-v1-AssignedJob)
    - [DockerSpec](#burst-v1-DockerSpec)
    - [GetJobStatusRequest](#burst-v1-GetJobStatusRequest)
    - [GetJobStatusResponse](#burst-v1-GetJobStatusResponse)
    - [JobSpec](#burst-v1-JobSpec)
    - [PollJobRequest](#burst-v1-PollJobRequest)
    - [PollJobResponse](#burst-v1-PollJobResponse)
    - [ProcessSpec](#burst-v1-ProcessSpec)
    - [PythonSpec](#burst-v1-PythonSpec)
    - [ReportJobResultRequest](#burst-v1-ReportJobResultRequest)
    - [ReportJobResultResponse](#burst-v1-ReportJobResultResponse)
    - [SubmitJobRequest](#burst-v1-SubmitJobRequest)
    - [SubmitJobResponse](#burst-v1-SubmitJobResponse)
  
- [burst/v1/worker.proto](#burst_v1_worker-proto)
    - [Empty](#burst-v1-Empty)
    - [HeartbeatRequest](#burst-v1-HeartbeatRequest)
    - [HeartbeatResponse](#burst-v1-HeartbeatResponse)
    - [RegisterWorkerRequest](#burst-v1-RegisterWorkerRequest)
    - [RegisterWorkerResponse](#burst-v1-RegisterWorkerResponse)
  
- [burst/v1/peer.proto](#burst_v1_peer-proto)
    - [StealJobsRequest](#burst-v1-StealJobsRequest)
    - [StealJobsResponse](#burst-v1-StealJobsResponse)
  
    - [WorkerPeerRpc](#burst-v1-WorkerPeerRpc)
  
- [Scalar Value Types](#scalar-value-types)



<a name="burst_v1_control-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## burst/v1/control.proto


 

 

 


<a name="burst-v1-ControllerRpc"></a>

### ControllerRpc
ControllerRpc defines the control-plane API used by CLI and workers.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| SubmitJob | [SubmitJobRequest](#burst-v1-SubmitJobRequest) | [SubmitJobResponse](#burst-v1-SubmitJobResponse) | SubmitJob enqueues a new job and returns the generated job id. |
| GetJobStatus | [GetJobStatusRequest](#burst-v1-GetJobStatusRequest) | [GetJobStatusResponse](#burst-v1-GetJobStatusResponse) | GetJobStatus returns the current lifecycle state for a job id. |
| RegisterWorker | [RegisterWorkerRequest](#burst-v1-RegisterWorkerRequest) | [RegisterWorkerResponse](#burst-v1-RegisterWorkerResponse) | RegisterWorker registers a worker instance and its available slots. |
| PollJob | [PollJobRequest](#burst-v1-PollJobRequest) | [PollJobResponse](#burst-v1-PollJobResponse) | PollJob returns either an assigned job or an explicit empty response. |
| ReportJobResult | [ReportJobResultRequest](#burst-v1-ReportJobResultRequest) | [ReportJobResultResponse](#burst-v1-ReportJobResultResponse) | ReportJobResult reports execution outcome for a previously assigned job. |
| Heartbeat | [HeartbeatRequest](#burst-v1-HeartbeatRequest) | [HeartbeatResponse](#burst-v1-HeartbeatResponse) | Heartbeat confirms worker liveness. |

 



<a name="burst_v1_job-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## burst/v1/job.proto



<a name="burst-v1-AssignedJob"></a>

### AssignedJob
AssignedJob is a job leased by the controller to a worker.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [string](#string) |  |  |
| spec | [JobSpec](#burst-v1-JobSpec) |  |  |






<a name="burst-v1-DockerSpec"></a>

### DockerSpec
DockerSpec executes a containerized command using `docker run`.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| image | [string](#string) |  | Docker image reference to run. |
| command | [string](#string) | repeated | Optional container command override. |
| args | [string](#string) | repeated | Arguments passed to the container command. |






<a name="burst-v1-GetJobStatusRequest"></a>

### GetJobStatusRequest
GetJobStatusRequest asks for the current state of a job.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [string](#string) |  |  |






<a name="burst-v1-GetJobStatusResponse"></a>

### GetJobStatusResponse
GetJobStatusResponse returns job id and current state.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [string](#string) |  |  |
| state | [string](#string) |  |  |






<a name="burst-v1-JobSpec"></a>

### JobSpec
JobSpec defines what to execute and where to store captured logs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| output_dir | [string](#string) | optional | Output directory for `&lt;job_id&gt;.stdout` and `&lt;job_id&gt;.stderr`. |
| process | [ProcessSpec](#burst-v1-ProcessSpec) |  |  |
| python | [PythonSpec](#burst-v1-PythonSpec) |  |  |
| docker | [DockerSpec](#burst-v1-DockerSpec) |  |  |






<a name="burst-v1-PollJobRequest"></a>

### PollJobRequest
PollJobRequest asks the controller for work for a worker.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| worker_id | [string](#string) |  |  |






<a name="burst-v1-PollJobResponse"></a>

### PollJobResponse
PollJobResponse returns either an assigned job or an empty marker.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [AssignedJob](#burst-v1-AssignedJob) |  |  |
| empty | [Empty](#burst-v1-Empty) |  |  |






<a name="burst-v1-ProcessSpec"></a>

### ProcessSpec
ProcessSpec executes a native process directly on the worker host.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [string](#string) |  | Executable path or command name. |
| args | [string](#string) | repeated | Command-line arguments passed to the process. |






<a name="burst-v1-PythonSpec"></a>

### PythonSpec
PythonSpec executes a Python entry point via the worker&#39;s python runtime.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entry_point | [string](#string) |  | Python entry point. Example: script path or &#39;-c&#39;. |
| args | [string](#string) | repeated | Arguments passed after the entry point. |






<a name="burst-v1-ReportJobResultRequest"></a>

### ReportJobResultRequest
ReportJobResultRequest reports completion status for an assigned job.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| worker_id | [string](#string) |  |  |
| job_id | [string](#string) |  |  |
| exit_code | [int32](#int32) |  | Process exit code. Convention: 0 means success. |
| error_message | [string](#string) |  | Human-readable error details when available. |






<a name="burst-v1-ReportJobResultResponse"></a>

### ReportJobResultResponse
ReportJobResultResponse acknowledges whether the result was accepted.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| accepted | [bool](#bool) |  |  |






<a name="burst-v1-SubmitJobRequest"></a>

### SubmitJobRequest
SubmitJobRequest sends a new job spec to the controller.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spec | [JobSpec](#burst-v1-JobSpec) |  |  |






<a name="burst-v1-SubmitJobResponse"></a>

### SubmitJobResponse
SubmitJobResponse returns the generated job id.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [string](#string) |  |  |





 

 

 

 



<a name="burst_v1_worker-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## burst/v1/worker.proto



<a name="burst-v1-Empty"></a>

### Empty
Empty is an explicit empty payload for oneof responses.






<a name="burst-v1-HeartbeatRequest"></a>

### HeartbeatRequest
HeartbeatRequest pings worker liveness.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| worker_id | [string](#string) |  |  |






<a name="burst-v1-HeartbeatResponse"></a>

### HeartbeatResponse
HeartbeatResponse indicates heartbeat acceptance.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ok | [bool](#bool) |  |  |






<a name="burst-v1-RegisterWorkerRequest"></a>

### RegisterWorkerRequest
RegisterWorkerRequest registers a worker with id and concurrency slots.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| worker_id | [string](#string) |  | Stable unique worker identifier. |
| slots | [uint32](#uint32) |  | Number of concurrent jobs this worker can accept. |
| queue_capacity | [uint32](#uint32) |  | Maximum queued&#43;running jobs worker can lease from controller. |






<a name="burst-v1-RegisterWorkerResponse"></a>

### RegisterWorkerResponse
RegisterWorkerResponse indicates whether registration succeeded.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| accepted | [bool](#bool) |  |  |





 

 

 

 



<a name="burst_v1_peer-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## burst/v1/peer.proto



<a name="burst-v1-StealJobsRequest"></a>

### StealJobsRequest
StealJobsRequest identifies the thief and requested steal size.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| thief_worker_id | [string](#string) |  |  |
| max_jobs | [uint32](#uint32) |  |  |






<a name="burst-v1-StealJobsResponse"></a>

### StealJobsResponse
StealJobsResponse returns zero or more stolen jobs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobs | [AssignedJob](#burst-v1-AssignedJob) | repeated |  |





 

 

 


<a name="burst-v1-WorkerPeerRpc"></a>

### WorkerPeerRpc
WorkerPeerRpc defines peer-to-peer work-stealing APIs between workers.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| StealJobs | [StealJobsRequest](#burst-v1-StealJobsRequest) | [StealJobsResponse](#burst-v1-StealJobsResponse) | StealJobs asks a peer worker for a bounded number of queued jobs. |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

