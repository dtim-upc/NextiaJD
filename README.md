

<h1 align="center">
  <a href="https://www.essi.upc.edu/dtim/nextiajd/"><img src="https://github.com/dtim-upc/spark/blob/nextiajd_v3.0.1/sql/nextiajd/img/logo.png?raw=true" alt="NextiaJD" width="300">
  </a>
</h1>

<h4 align="center">A Scalable Data Discovery solution using profiles based on <a href="https://spark.apache.org/" target="_blank">Apache Spark</a>.</h4>

<p align="center">
  <a href="#about">About</a> •
  <a href="#key-features">Key Features</a> •
  <a href="#how-it-works">How it works</a> •
  <a href="#usage">Usage</a> •
  <a href="#installation">Installation</a> •
  <a href="#demo-zeppelin-notebook">Demo</a> •
  <a href="#reproducibility-of-experiments">Reproducibility</a>
</p>

## About
**NextiaJD**, from <a href="https://nahuatl.uoregon.edu/content/nextia" target="_blank">*nextia*</a> in the <a href="https://en.wikipedia.org/wiki/Nahuatl" target="_blank">Nahuatl</a> language (the old Aztec language), is a scalable data discovery system. **NextiaJD** computes profiles, which are succint representations of the underlying characteristics of datasets and their attributes, to efficiently discover joinable attributes on datasets. We aim to automatically discover pairs of attributes in a massive collection of heterogeneous datasets (i.e., data lakes) that can be crossed.

Here, we provide you detailed information on how to run and evaluate NextiaJD. 

## Key features   
* Attribute profiling built-in Spark  
* A fully distributed end-to-end framework for joinable attributes discovery.  
* Easy data discovery for everyone  

## How it works

We encourage you to read our paper to better understand what NextiaJD is and how can fit your scenarios. 

The simple way to describe it: 

<div align="center">
 <img src="https://github.com/dtim-upc/spark/raw/nextiajd_v3.0.1/sql/nextiajd/img/example.gif?raw=true" alt="NextiaJD" width="300">
</div>

You have one dataset and a collection of independent datasets. Then, you will like to find other datasets with attributes that performed a high quality join.
 
NextiaJD reduces the effort to do a manual exploration by predicting which attributes are candidates for a join based on some qualities defined. 

We have as an example two scenarios:

* In a data lake when a new dataset is ingested,  a profile should be computed. Then, whenever a data analysts has a dataset, NextiaJD can find other datasets in the data lake that can be joined.
* In a normal repository,  when having a few datasets and we want to know how they can be crossed against one dataset.

## Requirements

* Spark >= 3.0.1
* Scala 2.12.*
* Java 8 or 11

## Installation

The easy way to use NextiaJD is with Maven. 

For SBT just add the following dependency in your build.sbt:

````
libraryDependencies += "edu.upc.essi.dtim.nextiajd" % "nextiajd_2.12" % "1.0.2"

````

For Apache Maven, just add the following dependency in your pom.xml:


````
<dependency>
  <groupId>edu.upc.essi.dtim.nextiajd</groupId>
  <artifactId>nextiajd_2.12</artifactId>
  <version>1.0.1</version>
</dependency>
````

For more ways to import it, please go <a href="https://central.sonatype.com/artifact/edu.upc.essi.dtim.nextiajd/nextiajd_2.12/1.0.2">here</a>

    
## Usage    
         
To start using NextiaJD just import the implicits class as below:

```
import edu.upc.essi.dtim.NextiaJD.implicits
```         
### Attribute profiling  
  
Then to start an attribute profile you need to call the method `attProfile()` from a DataFrame object. By default, once a profile is computed, it will be saved in the dataset directory. This allows to reuse the profile for future discoveries without having to compute it again. While you can use any dataset format, we recommend to use parquet files to compute profiles faster.
  
``` 
val dataset = spark.read.csv(...)  
# computes attribute profile
val profile = dataset.attProfile() 
# show the profile computed from dataset DataFrame
profile.show()   
```  
  
### Join Discovery  
  
Our Join Discovery is focused on the quality result of a join statement. Thus, we defined a totally-ordered set of quality classes:

* **High**: attributes pair with a containment similarity of 0.75 and a maximum cardinality proportion of 4.    
* **Good**: attributes pair with a containment similarity of 0.5 and a maximum cardinality proportion of 8.     
* **Moderate**: attributes pair with a containment similarity of 0.25 and a maximum cardinality proportion of 12.     
* **Poor**: attributes pair with a containment similarity of 0.1    
* **None**: otherwise   

You can start a discovery by using the function `discovery()` from a DataFrame object. As an example the following code will start a discovery to find any attribute from our dataset that can be used for a join with some dataset from the repository.
  
```  
val dataset = spark.read.csv(...)  
val repository = # list of candidate datasets to be compared against dataset

#Start discovery, you can also do a discovery by attribute by adding the attribute name in the discovery method such as discovery(respository, "attributeName")
val discovery = dataset.discovery(respository)

discovery.show

+-------------------+---------------+--------------------+-------------------+-------+------------------+
|      query dataset|query attribute|   candidate dataset|candidate attribute|quality|       probability|
+-------------------+---------------+--------------------+-------------------+-------+------------------+
|00_Emma_dataset.csv|        Capital|lista_comunidades...|            Capital|   High|0.5916666666666667|
+-------------------+---------------+--------------------+-------------------+-------+------------------+
```    

By default, we just show candidates attributes that performs a High and Good quality joins. If you want to explore Moderate and Poor results, you have to add a suffix to the discovery method such as `discoveryPoor` and `discoveryModerate` that will only show results for the specified quality.  

## Demo (Zeppelin Notebook) 

Check out the [demo project](http://dtim.essi.upc.edu:8081/#/notebook/2G9CMU9C4) for a quick example of how NextiaJD works. Bear in mind that, in order to access them you must first login with the following credentials (user: user1, password: nextiajd).

Note that we also have a step by step notebook which can also be found [here](http://dtim.essi.upc.edu:8081/#/notebook/2FZ5HCMJQ)

More information and a video can be found [here](https://www.essi.upc.edu/~jflores/nextiajd.html#demonstration)
 
## Reproducibility of Experiments

We performed differents experiments to evaluate the predictive performance and efficiency of NextiaJD. In the spirit of open research and experimental reproducibility, we provide detailed information on how to reproduce them. 
