name := "spark-data-prep"
version := "1.0.0"
scalaVersion := "2.12.18"

// Spark 버전
val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",

  // CLI 파싱
  "com.github.scopt" %% "scopt" % "4.1.0",

  // 테스트
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
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
