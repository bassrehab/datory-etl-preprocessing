![Datory Framework](./img/datory.png)

Datory Pre-Processing Framework
=========================


Introduction
------------
This is an extension of the Datory Platform framework that supports pre-processing of files.

Background
----------
Pre-processing comprises of two standard actions:
- New Line Management (/n, /r, /r/n line endings found within a row - resulting in an unintended breaks). 
For example consider this, FILE_ABC is expected to have the following "well-formed" structure with 4 columns:

```
Expected Format
===============
123, 0909, cat, NV
234, 0190, dog, CA

Actual file received
====================
123, 0909, 
cat, NV
234, 
0190, 
dog, CA
```
- Control Character Replacement: Non ASCII Characters encountered from mainframes, varying encodings etc.

Broad Design Goals
------------------
- Allow distributed pre-processing of very large files (non *nix utilities based)
- Create a scalable and extensible framework

Approach
--------
- Define custom Hadoop Input Formats, with Line Record Reader. These input formats can then be leveraged in MapReduce Computation paradigms or using Apache Spark.
- Delimited format has been implemented specifically, and other formats may be extended using this framework.
- Follows the pipeline design pattern

Project Structure
-----------------

The project follows a multi module maven project workflow.
- Client Module
- Core Module
- Common Module
- Model Module
- Pipeline Module
- Utils Module
- Bridge Module

Modules
-------

**Client Module**

Entry point is `Application`, that implements the interface 
`HookFrameworkInterface` and `execute` method.
The Client Module is used to launch the spark job on the cluster using `SparkJobLauncher`.
Needs `client.application.properties` file

**Core Module**

This module contains the core logic for pre-processing. 
This JAR is distributed to the cluster as part of the `SparkJobLauncher` invocation.

Four Pipelined Stages are defined:
- Stage 1: `StageApplicationInitialize`
- Stage 2: `StageFetchMetadata`
- Stage 3: `StageFileProcessing`
- Stage 4: `StageApplicationFinalize`

Stage 3 forms the key stage. Pre-processing is currently implemented for DELIMITED Files. 
The Scala Class, `com.subhadipmitra.datory.preprocessing.core.formats.delimited.DelimitedFileProcessor` 
handles the new line management and Control Char replacement.

Additional Formats may be handled by defining the similar Processors. 
Hadoop Custom Input format based templates are provided for delimited and generic regex based implementation in 
package `com.subhadipmitra.datory.preprocessing.core.formats.delimited`

Needs `cluster.application.properties` file.

**Common Module**
Contains common metadata DB connection, config reader, application constants, exception messages.


**Model Module**
- `PayloadModel`: Payload Wrapper Model, that contains access to all other sub models
- `ParamsModel`: Model that packs the parameters received by the Client and Core Module 
- `LayoutModel`: File Layout Model, currently implemented for DELIMITED files
- `DestinationModel`: Hadoop Destination Table model
- `ResponseModel`: Response object model returned to the caller upstream
- `SourceModel`: Landing folder specific model from where the pre-processing file is picked up
- `SparkJobModel`: Spark Job Parameters Model, used by Client Module to launch the spark job.
- `StatusModel`: Mostly used for collating stagewise logs of the Core-Cluster Module.



**Pipeline Module**
Framework that implements the Pipeline processing pattern. See, Core Module.



**Utils Module**
Generic Utilities module. Includes, Hadoop, JSON and Spark specific utils.

**Bridge Module**
For compilation of no Java/Scala code. Implemented bridge includes currently for Python.


Setup
-----

**Companion Deployment Shell Scripts**
For copying the property file `cluster.application.properties` file to HDFS, to be used by Core Module.
Refer folder `sbin` for more.

**Metadata Entries**
DATORY_PROC_PARAM and DATORY_PROC_PRE_PROCESSING_FLAG tables must have corresponding entries for the PROC_ID.

Todo
----
- Better Docs
- Support Apache Spark 3.x
- Extend to popular obj stores.