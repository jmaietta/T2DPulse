<#
.SYNOPSIS
    Cleans up old permalinks and timestamped JSON files from T2DPulse.

.DESCRIPTION
    This script removes:
    - Permalink directories under docs/p that are older than 3 days
    - JSON files under docs/archive/timestamped that are older than 3 days

    Age is determined by the file/folder's LastWriteTime (last modified time).

.PARAMETER DryRun
    When specified, shows what would be deleted without actually deleting anything.

.PARAMETER MaxAgeDays
    Number of days after which files are considered old. Default is 3.

.EXAMPLE
    .\cleanup-old-files.ps1 -DryRun
    Shows what would be deleted without making changes.

.EXAMPLE
    .\cleanup-old-files.ps1
    Performs the actual cleanup, deleting old files.

.EXAMPLE
    .\cleanup-old-files.ps1 -MaxAgeDays 7
    Deletes files older than 7 days instead of the default 3.
#>

[CmdletBinding()]
param(
    [switch]$DryRun,
    [int]$MaxAgeDays = 3
)

# Determine script and repo paths
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir

# Target directories (relative to repo root)
$PermalinksDir = Join-Path $RepoRoot "docs\p"
$TimestampedDir = Join-Path $RepoRoot "docs\archive\timestamped"
$LogDir = Join-Path $RepoRoot "docs\archive\cleanup-logs"

# Calculate cutoff date
$CutoffDate = (Get-Date).AddDays(-$MaxAgeDays)
$Timestamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$LogFile = Join-Path $LogDir "cleanup_$Timestamp.log"

# Statistics
$DeletedPermalinks = 0
$DeletedTimestamped = 0
$ErrorCount = 0

function Write-Log {
    param(
        [string]$Message,
        [string]$Level = "INFO"
    )

    $LogTimestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogEntry = "[$LogTimestamp] [$Level] $Message"

    # Write to console
    switch ($Level) {
        "ERROR" { Write-Host $LogEntry -ForegroundColor Red }
        "WARN"  { Write-Host $LogEntry -ForegroundColor Yellow }
        "DRY"   { Write-Host $LogEntry -ForegroundColor Cyan }
        default { Write-Host $LogEntry }
    }

    # Write to log file (only in real-run mode)
    if (-not $DryRun) {
        Add-Content -Path $LogFile -Value $LogEntry -ErrorAction SilentlyContinue
    }
}

function Initialize-LogDirectory {
    if (-not $DryRun) {
        if (-not (Test-Path $LogDir)) {
            New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
            Write-Log "Created log directory: $LogDir"
        }
    }
}

function Remove-OldPermalinks {
    Write-Log "Processing permalinks directory: $PermalinksDir"
    Write-Log "Cutoff date: $CutoffDate (files older than $MaxAgeDays days)"

    if (-not (Test-Path $PermalinksDir)) {
        Write-Log "Permalinks directory does not exist: $PermalinksDir" -Level "WARN"
        return
    }

    # Get all subdirectories in docs/p
    $Directories = Get-ChildItem -Path $PermalinksDir -Directory -ErrorAction SilentlyContinue

    foreach ($Dir in $Directories) {
        # Check if directory is older than cutoff
        if ($Dir.LastWriteTime -lt $CutoffDate) {
            $script:DeletedPermalinks++

            if ($DryRun) {
                Write-Log "[DRY-RUN] Would delete permalink: $($Dir.Name) (Last modified: $($Dir.LastWriteTime))" -Level "DRY"
            } else {
                try {
                    Remove-Item -Path $Dir.FullName -Recurse -Force -ErrorAction Stop
                    Write-Log "Deleted permalink: $($Dir.Name) (Last modified: $($Dir.LastWriteTime))"
                } catch {
                    $script:ErrorCount++
                    Write-Log "Failed to delete $($Dir.FullName): $_" -Level "ERROR"
                }
            }
        }
    }
}

function Remove-OldTimestampedFiles {
    Write-Log "Processing timestamped directory: $TimestampedDir"
    Write-Log "Cutoff date: $CutoffDate (files older than $MaxAgeDays days)"

    if (-not (Test-Path $TimestampedDir)) {
        Write-Log "Timestamped directory does not exist: $TimestampedDir" -Level "WARN"
        return
    }

    # Get all .json files in docs/archive/timestamped
    $JsonFiles = Get-ChildItem -Path $TimestampedDir -Filter "*.json" -File -ErrorAction SilentlyContinue

    foreach ($File in $JsonFiles) {
        # Check if file is older than cutoff
        if ($File.LastWriteTime -lt $CutoffDate) {
            $script:DeletedTimestamped++

            if ($DryRun) {
                Write-Log "[DRY-RUN] Would delete timestamped file: $($File.Name) (Last modified: $($File.LastWriteTime))" -Level "DRY"
            } else {
                try {
                    Remove-Item -Path $File.FullName -Force -ErrorAction Stop
                    Write-Log "Deleted timestamped file: $($File.Name) (Last modified: $($File.LastWriteTime))"
                } catch {
                    $script:ErrorCount++
                    Write-Log "Failed to delete $($File.FullName): $_" -Level "ERROR"
                }
            }
        }
    }
}

# Main execution
Write-Log "========================================"
Write-Log "T2DPulse Cleanup Script"
Write-Log "========================================"

if ($DryRun) {
    Write-Log "MODE: DRY-RUN (no files will be deleted)" -Level "DRY"
} else {
    Write-Log "MODE: REAL-RUN (files will be deleted)"
    Initialize-LogDirectory
    Write-Log "Log file: $LogFile"
}

Write-Log "Repository root: $RepoRoot"
Write-Log "Max age: $MaxAgeDays days"
Write-Log "----------------------------------------"

# Perform cleanup
Remove-OldPermalinks
Write-Log "----------------------------------------"
Remove-OldTimestampedFiles

# Summary
Write-Log "========================================"
Write-Log "CLEANUP SUMMARY"
Write-Log "========================================"

if ($DryRun) {
    Write-Log "Permalinks that would be deleted: $DeletedPermalinks" -Level "DRY"
    Write-Log "Timestamped files that would be deleted: $DeletedTimestamped" -Level "DRY"
    Write-Log "Total items that would be deleted: $($DeletedPermalinks + $DeletedTimestamped)" -Level "DRY"
} else {
    Write-Log "Permalinks deleted: $DeletedPermalinks"
    Write-Log "Timestamped files deleted: $DeletedTimestamped"
    Write-Log "Total items deleted: $($DeletedPermalinks + $DeletedTimestamped)"
    Write-Log "Errors encountered: $ErrorCount"

    if ($ErrorCount -gt 0) {
        Write-Log "Some errors occurred during cleanup. Check the log for details." -Level "WARN"
    }
}

Write-Log "========================================"
Write-Log "Cleanup completed at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Log "========================================"

# Exit with appropriate code
if ($ErrorCount -gt 0) {
    exit 1
}
exit 0
