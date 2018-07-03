
# SparkML-DAG

Implementation of a DAG Pipeline for SparkML.

# Motivation

This library extends SparkML to allow for Pipelines that are DAG based. 
That is multiple input datasets can be manipulated to create complex models. 
One such example can be seen in the [test](src/test/scala/org/apache/spark/ml/feature/dag/DAGPipelineTest.scala).

# Development

Clone this repository and run `mvn clean test`

To build for a custom version of Spark/Scala, run 
`mvn clean package \
-Dscala.major.version=<SCALA_MAJOR> \
-Dscala.minor.version=<SCALA_MINOR>\
-Dspark.version=<SPARK_VERSION>`

e.g. 
```bash
mvn clean package \
-Dscala.major.version=2.11 \
-Dscala.minor.version=2.11.8 \
-Dspark.version=2.3.0
```

## build profiles

Alternatively one can build against a limited number of pre-defined profiles.
See the [pom](pom.xml) for a list of the profiles.

Example build with profiles: 

`mvn clean package -Pspark_2.3,scala_2.11`

`mvn clean package -Pspark_2.0,scala_2.10`


# Support

Here is a handy table of supported build version combinations:

| Apache Spark | Scala |
|:------------:|:-----:|
| 2.0.x        | 2.10  |
| 2.0.x        | 2.11  | 
| 2.1.x        | 2.10  |
| 2.1.x        | 2.11  |
| 2.2.x        | 2.10  |
| 2.2.x        | 2.11  |
| 2.3.x        | 2.11  |

# License

see the [license](LICENSE) for license information.
