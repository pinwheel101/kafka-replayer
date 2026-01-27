name := "spark-data-prep"
version := "1.0.0"
scalaVersion := "2.12.18"

// Spark 버전
val sparkVersion = "3.5.0"

// Serialization 버전
val apicurioVersion = "2.5.8.Final"
val avroVersion = "1.11.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",

  // CLI 파싱
  "com.github.scopt" %% "scopt" % "4.1.0",

  // Avro support
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
  "org.apache.avro" % "avro" % avroVersion,

  // Apicurio Schema Registry client
  "io.apicurio" % "apicurio-registry-serdes-avro-serde" % apicurioVersion,
  "io.apicurio" % "apicurio-registry-client" % apicurioVersion,

  // 테스트
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.testcontainers" % "testcontainers" % "1.19.3" % Test,
  "org.testcontainers" % "kafka" % "1.19.3" % Test,
  "org.testcontainers" % "postgresql" % "1.19.3" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-kafka" % "0.41.0" % Test
)

// Assembly 설정
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar"

// Scala 컴파일 옵션
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked"
)
