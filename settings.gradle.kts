plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}
rootProject.name = "flowx"

include("common")
include("flowx:core")
include("flowx:demo")
include("flowx:redis")
include("k8demo:ingress")
include("k8demo:node")
