$ErrorActionPreference = 'Stop'
$packages = @(
    @('forest-qwen-vl-embedding', 'package-qwen-vl-embedding'),
    @('forest-openai-provider', 'package-openai-provider'),
    @('forest-context-module', 'package-context'),
    @('forest-memory-module', 'package-memory'),
    @('forest-deepseek-web', 'package-deepseek-web'),
    @('forest-agent-tools', 'package-agent-tools'),
    @('forest-agent-module', 'package-agent')
)
foreach ($package in $packages) {
    cargo run --locked --offline -j 2 -p $package[0] --bin $package[1]
    if ($LASTEXITCODE -ne 0) { throw "Package build failed: $($package[0])" }
}
