<#
.SYNOPSIS
  Cleans up old generated files in this repo.

.DESCRIPTION
  - Deletes *files* under docs/p (permalinks) older than N days.
    (Does NOT delete folders.)
  - Deletes *.json files under docs/archive/timestamped older than N days.

  Designed to be run from Windows Task Scheduler.

.PARAMETER DaysToKeep
  Files older than this many days are deleted.

.PARAMETER DryRun
  If set, prints what would be deleted but makes no changes.

.PARAMETER ExcludeNames
  File names to never delete (defaults include .gitkeep so empty dirs stay in git).

.EXAMPLE
  .\cleanup-old-files.ps1 -DaysToKeep 3

.EXAMPLE
  .\cleanup-old-files.ps1 -DaysToKeep 3 -DryRun
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory = $false)]
  [ValidateRange(1, 3650)]
  [int]$DaysToKeep = 3,

  [Parameter(Mandatory = $false)]
  [switch]$DryRun,

  [Parameter(Mandatory = $false)]
  [string[]]$ExcludeNames = @('.gitkeep', '.gitignore', 'README.md')
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-ScriptDirectory {
  if ($PSScriptRoot) { return $PSScriptRoot }
  return (Split-Path -Parent $MyInvocation.MyCommand.Path)
}

function Get-RepoRoot([string]$ScriptDir) {
  # scripts/ is expected to be at repo root
  $candidate = Join-Path $ScriptDir '..'
  return (Resolve-Path -LiteralPath $candidate).Path
}

function Get-LogDirectory([string]$RepoRoot) {
  # Prefer per-user, non-repo location to avoid dirtying the git working tree.
  if ($env:LOCALAPPDATA) {
    return (Join-Path $env:LOCALAPPDATA 'T2DPulse\cleanup-logs')
  }

  # Fallback (rare): keep logs next to scripts
  return (Join-Path $RepoRoot 'scripts\cleanup-logs')
}

function Write-Log([string]$Message) {
  $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  $line = "[$timestamp] $Message"

  Write-Host $line

  try {
    if (-not (Test-Path -LiteralPath $script:LogDir)) {
      New-Item -ItemType Directory -Path $script:LogDir -Force | Out-Null
    }

    Add-Content -LiteralPath $script:LogFile -Value $line
  }
  catch {
    # Logging should never crash cleanup.
    Write-Host "[WARN] Failed to write log: $($_.Exception.Message)"
  }
}

$ScriptDir = Get-ScriptDirectory
$RepoRoot = Get-RepoRoot -ScriptDir $ScriptDir

$PermalinksDir  = Join-Path $RepoRoot 'docs\p'
$TimestampedDir = Join-Path $RepoRoot 'docs\archive\timestamped'

$Cutoff = (Get-Date).AddDays(-$DaysToKeep)

$LogDir  = Get-LogDirectory -RepoRoot $RepoRoot
$LogFile = Join-Path $LogDir ("cleanup-{0}.log" -f (Get-Date -Format 'yyyyMMdd'))

Write-Log "Starting cleanup (RepoRoot='$RepoRoot', DaysToKeep=$DaysToKeep, Cutoff='$Cutoff', DryRun=$DryRun)"

$errors = @()

function Remove-OldFilesInFolder([string]$Folder, [scriptblock]$Selector, [string]$Label) {
  if (-not (Test-Path -LiteralPath $Folder)) {
    Write-Log "[$Label] Folder not found; skipping: $Folder"
    return @{ Deleted = 0; Candidates = 0 }
  }

  $candidates = @()
  try {
    $candidates = & $Selector
  }
  catch {
    $errors += "[$Label] Failed to enumerate files in '$Folder': $($_.Exception.Message)"
    Write-Log $errors[-1]
    return @{ Deleted = 0; Candidates = 0 }
  }

  $toDelete = $candidates | Where-Object {
    $_.LastWriteTime -lt $Cutoff -and ($ExcludeNames -notcontains $_.Name)
  }

  $deleted = 0
  foreach ($file in $toDelete) {
    try {
      if ($DryRun) {
        Write-Log "[$Label] DRY RUN: Would delete file: $($file.FullName)"
      }
      else {
        Remove-Item -LiteralPath $file.FullName -Force
        Write-Log "[$Label] Deleted file: $($file.FullName)"
      }
      $deleted++
    }
    catch {
      $errors += "[$Label] Error deleting '$($file.FullName)': $($_.Exception.Message)"
      Write-Log $errors[-1]
    }
  }

  Write-Log "[$Label] Candidates=$($candidates.Count), MarkedForDeletion=$($toDelete.Count), DeletedOrWouldDelete=$deleted"
  return @{ Deleted = $deleted; Candidates = $candidates.Count }
}

# 1) docs/p : delete FILES only (do not delete folders)
$permalinkStats = Remove-OldFilesInFolder -Folder $PermalinksDir -Label 'docs/p' -Selector {
  Get-ChildItem -LiteralPath $PermalinksDir -File -Recurse -Force
}

# 2) docs/archive/timestamped : delete JSON FILES only
$timestampStats = Remove-OldFilesInFolder -Folder $TimestampedDir -Label 'docs/archive/timestamped' -Selector {
  Get-ChildItem -LiteralPath $TimestampedDir -File -Recurse -Force -Filter '*.json'
}

Write-Log "Finished cleanup. docs/p deleted/would-delete=$($permalinkStats.Deleted); timestamped deleted/would-delete=$($timestampStats.Deleted); errors=$($errors.Count)"

if ($errors.Count -gt 0) {
  Write-Error ("Cleanup completed with {0} error(s). See log: {1}" -f $errors.Count, $LogFile)
  exit 1
}

exit 0
