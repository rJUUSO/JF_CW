param(
    [string]$Remote = "origin",
    [switch]$UpdateExisting
)

# Create and push a date-based release tag (vYYYY.MM.DD, UTC). Pushing the tag
# triggers .github/workflows/release.yml, which builds and publishes the
# firmware, the LittleFS images and a full binary to a GitHub Release.
# One release a day: -UpdateExisting moves today's tag to the current commit.

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $repoRoot
try {
    $utcVersion = (Get-Date).ToUniversalTime().ToString("yyyy.MM.dd")
    $tag = "v$utcVersion"

    Write-Host "Release tag: $tag"

    git fetch --tags $Remote | Out-Null

    git show-ref --verify --quiet "refs/tags/$tag"
    $exists = ($LASTEXITCODE -eq 0)

    if ($exists -and -not $UpdateExisting) {
        Write-Host "Tag already exists: $tag"
        Write-Host "Use -UpdateExisting to move and republish the same-day release tag."
        exit 0
    }

    if ($exists -and $UpdateExisting) {
        git tag -f $tag
        git push -f $Remote $tag
        Write-Host "Updated and force-pushed tag: $tag"
        exit 0
    }

    git tag $tag
    git push $Remote $tag
    Write-Host "Created and pushed tag: $tag"
} finally {
    Pop-Location
}
