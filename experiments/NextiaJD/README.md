# Experiment 1: Predictive accuracy of NextiaJD

Here, we describe the steps needed to reproduce the experiments that evaluate the performance of NextiaJD. There are two ways to run this experiment, 1) Running the project as any sbt project or 2) running it using spark-submit.

## 1) Running as sbt project

### Steps

* Clone NextiaJD into your computer.
* Go to the directory `NextiaJD/experiments/NextiaJD/` 
   * This directory is a normal sbt project. Therefore you can also open it in an IDE as intellij IDEA or any other.
   * If you want sbt to download Apache Spark libraries, you will need to remove `% provided` command in [build.sbt](https://github.com/dtim-upc/NextiaJD/blob/main/experiments/NextiaJD/build.sbt) file.
* Then, you just have to run the class **NextiaJD_evaluation** (below of the table you have an example of the command to run it with sbt) providing some of the following parameters:


| Parameter           | Required | Description                                                                                                 |
|---------------------|----------|-------------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | True     | path to datasets info file. This file containsthe datasets names and their configuration to read them       |
| -g, --groud-truth   | True     | path to the ground truth csv file                                                                           |
| -o, --output        | True     | path to write the discovery and time results                                                                |
| -p, --path-datasets | True     | path to the folder containing all datasets                                                                  |
| -q, --query-type    | False    | The query search. There are two types:querybydataset and querybyattribute. Default value is querybydataset. |
| -t, --testbed       | False    | testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated. Default is ""      |
| -h, --help          | False    | Show help message                                                                                           |


* An example of how to run it with sbt command is the following:
```
sbt "runMain NextiaJD_evaluation -d /Users/javierflores/Downloads/testbedXS/datasetInformation_testbedXS.csv -p /Users/javierflores/Downloads/testbedXS/datasets -g /Users/javierflores/Downloads/testbedXS/groundTruth_testbedXS.csv -o /Users/javierflores/Downloads/testbedXS/output"
```


## 2) Running with spark-submit

### Prerequisites

* The spark-submit script. You can find this script in your Spark installation under the bin folder e.g $SPARK_HOME/bin
* A testbed provided for the experiment. See [this link](https://github.com/dtim-upc/NextiaJD/tree/1.0/experiments#setting) for more information.
* [NextiaJD_experiments.jar](https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download). This JAR should be run with `spark-submit` and we will use the following classes:
    *  **NextiaJD_evaluation** will compute the predictive accuracy: generates the discovery and time execution for the testbed. Note, this jar already contains the nextiajd dependency, therefore, we do not need to install it.

### Running Predictive accuracy

To run this experiment, you need to execute the class **NextiaJD_evaluation** from the [JAR]( https://mydisk.cs.upc.edu/s/WPp7ApMzeyPc7sX/download). You can see below the parameters needed for this class.


| Parameter           | Required | Description                                                                                                 |
|---------------------|----------|-------------------------------------------------------------------------------------------------------------|
| -d, --datasets-info | True     | path to datasets info file. This file containsthe datasets names and their configuration to read them       |
| -g, --groud-truth   | True     | path to the ground truth csv file                                                                           |
| -o, --output        | True     | path to write the discovery and time results                                                                |
| -p, --path-datasets | True     | path to the folder containing all datasets                                                                  |
| -q, --query-type    | False    | The query search. There are two types:querybydataset and querybyattribute. Default value is querybydataset. |
| -t, --testbed       | False    | testbed type: XS, S, M, L. It will be used to write a suffix in the filenames generated. Default is ""      |
| -h, --help          | False    | Show help message                                                                                           |

### Run

* Go to your Spark installation under the bin folder e.g $SPARK_HOME/bin
* Open the terminal
* Run it using the below command. Note that you should replace the parameters by your own directories. The parameter `spark.driver.memory` is added to the spark-submit command since the default value could result in GC overhead limit exceeded exception.

```
spark-submit \
--class NextiaJD_evaluation \
--master local[*]  /DTIM/nextiajd-experiments.jar \
--conf spark.driver.memory=5g \
-d /DTIM/testbedXS/datasetInformation_testbedXS.csv \
-g /DTIM/testbedXS/groundTruth_testbedXS.csv \
-p /DTIM/datasets -o /DTIM/output
```
* Once the program ends. You can find the following files in the provided output directory.
    *  **NextiaJD_testbed.csv**: this file contains the discovery results and the ground truth
    *  **Nextia\_evaluation_testbed.csv**: this file contains the confusion matrix and its metrics
    *  **time_testbed.txt**: this file contains the times from the execution: pre runtime and runtime
