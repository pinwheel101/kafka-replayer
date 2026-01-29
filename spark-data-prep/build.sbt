name := "spark-data-prep"
version := "1.0.0"
scalaVersion := "2.13.16"

// Spark 버전
val sparkVersion = "4.0.1"

// Serialization 버전
val apicurioVersion = "3.1.6"
val avroVersion = "1.11.3"
val protobufVersion = "3.25.3"

// Jackson 버전 (Spark 4.0.1과 호환)
val jacksonVersion = "2.18.2"

libraryDependencies ++= Seq(
  // Jackson dependencies - 버전 명시로 충돌 방지
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",

  // CLI 파싱
  "com.github.scopt" %% "scopt" % "4.1.0",

  // Avro support
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
  "org.apache.avro" % "avro" % avroVersion,

  // Protobuf support
  "org.apache.spark" %% "spark-protobuf" % sparkVersion % "provided",
  "com.google.protobuf" % "protobuf-java" % protobufVersion,
  "com.google.protobuf" % "protobuf-java-util" % protobufVersion,

  // Apicurio Schema Registry client (3.x)
  "io.apicurio" % "apicurio-registry-avro-serde-kafka" % apicurioVersion,
  "io.apicurio" % "apicurio-registry-java-sdk" % apicurioVersion,

  // 테스트
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.testcontainers" % "testcontainers" % "1.19.3" % Test,
  "org.testcontainers" % "kafka" % "1.19.3" % Test,
  "org.testcontainers" % "postgresql" % "1.19.3" % Test,
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.0" % Test,
  "com.dimafeng" %% "testcontainers-scala-kafka" % "0.41.0" % Test
)

// Jackson 버전 강제 적용
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
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
