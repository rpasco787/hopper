rootProject.name = "hopper-java"

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories { mavenCentral() }
}

include("hopper-proto", "hopper-client", "hopper-worker", "hopper-example-worker")
