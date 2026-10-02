# Deployment probe example

This is an operator-installed deployment package. It publishes one read-only Worker service, a management query and its own WebUI panel. It has no private SQL, write port, Effect or requested capability.

Run `forest plugin check examples/deployment_probe --config-json '{}'` before packaging. Build or import the immutable artifact into the same `FOREST_PACKAGE_CATALOG_ROOT` volume used by the Supervisor, then copy the returned `package_digest`. In the deployment scope's RuntimeGraph panel choose **新增**, enter `example.deployment-probe` and that digest. The `probe.echo` panel appears after the queued graph publication succeeds.

To hot reload it, change the package version and Worker code, import the new artifact and choose **替换** for the existing component. Package versions cannot silently change their bytes. The current management console cannot upload ZIP files; catalog import still requires the CLI or an operator-managed artifact path.
