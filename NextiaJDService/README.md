# NextiaJD API

This is an API that wraps NextiaJD's services so they can be used from other programming languages (e.g., Python) invoking the command via terminal.

## Installation

### Requirements
- Scala 2.12
- sbt
- Java 11

### Compiling the project

From the root of the project run

```sh
$ sbt assembly
```

This will generate a JAR file `target/scala-2.12/NextiaJDService-assembly-0.1.jar`

## Usage

### Data profiling

#### From CSV file

```sh
$ java -jar target/scala-2.12/NextiaJDService-assembly-0.1.jar --profile --path in.csv --output ABCDProfile
```

#### From data in command line

```sh
$ java -jar target/scala-2.12/NextiaJDService-assembly-0.1.jar --profile --data A,B,C,D --output ABCDProfile
```

The generated profile will be stored in a directory `ABCDProfile`, with a single-partition JSON file (e.g., `part-00000-f421ea29-5879-4e26-9913-2a331bae9192-c000.json`)

### Distance vector computation

#### From CSV file

```sh
$ java -jar target/scala-2.12/NextiaJDService-assembly-0.1.jar --computeDistances --pathA in_A.csv -pathB in_B.csv --output distances
```

#### From data in command line

```sh
$ java -jar target/scala-2.12/NextiaJDService-assembly-0.1.jar --computeDistances --dataA A,B,C,D --dataB C,D,E,F --output distances
```

The generated profile will be stored in a directory `distances`, with a single-partition JSON file (e.g., `part-00000-f421ea29-5879-4e26-9913-2a331bae9192-c000.json`)
