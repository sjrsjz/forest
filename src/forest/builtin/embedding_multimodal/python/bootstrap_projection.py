"""Creation-time export from the exact Embedding package artifact."""

from space_identity import digest_json, profile_snapshot


def project(configuration):
    profile = configuration["profile"]
    return {
        "space_id": digest_json(profile_snapshot(profile)),
        "dimensions": profile["dimensions"],
    }
