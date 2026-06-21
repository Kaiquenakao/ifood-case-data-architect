def getResolvedOptions(argv, options):
    defaults = {
        "JOB_NAME": "mock_job",
        "BUCKET_NAME": "mock-bucket",
    }
    return {opt: defaults.get(opt, f"mock_{opt}") for opt in options}
