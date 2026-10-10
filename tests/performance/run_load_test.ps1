# PulseStream — Automated JMeter Performance Test Runner
param(
    [string]$HostName = "localhost",
    [int]$Port = 8081,
    [int]$Threads = 10,
    [int]$Loops = 50
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " PulseStream Milestone 8: JMeter Performance Load Test    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Verify Application Health
Write-Host "[1/4] Checking target application health at http://${HostName}:${Port}/actuator/health..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "http://${HostName}:${Port}/actuator/health" -TimeoutSec 5
    if ($health.status -ne "UP") {
        Write-Error "Application is not UP: $($health | ConvertTo-Json)"
    }
    Write-Host " -> Application is healthy (status: $($health.status))" -ForegroundColor Green
} catch {
    Write-Error "Could not connect to application on port ${Port}. Ensure Spring Boot is running."
}

# 2. Prepare Output Directories
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TestPlan = Join-Path $ScriptDir "pulsestream_load_test.jmx"
$ResultsDir = Join-Path $ScriptDir "results"
$JtlFile = Join-Path $ResultsDir "load_test_results.jtl"
$HtmlReportDir = Join-Path $ResultsDir "html_report"

if (-not (Test-Path $TestPlan)) {
    Write-Error "JMeter test plan not found: $TestPlan"
}

if (Test-Path $ResultsDir) {
    Write-Host "[2/4] Cleaning previous test results..." -ForegroundColor Yellow
    Remove-Item -Path $ResultsDir -Recurse -Force
}
New-Item -ItemType Directory -Path $ResultsDir -Force | Out-Null

# 3. Execute JMeter in CLI Mode
Write-Host "[3/4] Launching Apache JMeter non-GUI load test..." -ForegroundColor Yellow
Write-Host " -> Target: http://${HostName}:${Port}"
Write-Host " -> Ingestion Threads: $Threads, Loops: $Loops"

$jmeterCmd = "d:\apache-jmeter-5.6.3\bin\jmeter.bat"
if (-not (Test-Path $jmeterCmd)) {
    $jmeterCmd = (Get-Command jmeter.bat -ErrorAction Stop).Source
}

$args = @(
    "-n",
    "-t", $TestPlan,
    "-l", $JtlFile,
    "-e",
    "-o", $HtmlReportDir,
    "-Jhost=$HostName",
    "-Jport=$Port",
    "-Jthreads=$Threads",
    "-Jloops=$Loops"
)

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
& $jmeterCmd $args
$stopwatch.Stop()

Write-Host " -> JMeter execution completed in $($stopwatch.Elapsed.TotalSeconds.ToString('F2')) seconds" -ForegroundColor Green

# 4. Parse Results & Calculate Performance Metrics
Write-Host "[4/4] Parsing execution metrics from $JtlFile..." -ForegroundColor Yellow

if (-not (Test-Path $JtlFile)) {
    Write-Error "JTL results file was not generated."
}

$csv = Import-Csv -Path $JtlFile
$totalRequests = $csv.Count
$elapsedValues = $csv | ForEach-Object { [int]$_.elapsed } | Sort-Object
$failures = ($csv | Where-Object { $_.success -ne "true" }).Count

function Get-Percentile($arr, $p) {
    if ($arr.Count -eq 0) { return 0 }
    $index = [int][Math]::Ceiling(($p / 100.0) * $arr.Count) - 1
    return $arr[[Math]::Max(0, [Math]::Min($index, $arr.Count - 1))]
}

$p50 = Get-Percentile $elapsedValues 50
$p90 = Get-Percentile $elapsedValues 90
$p95 = Get-Percentile $elapsedValues 95
$p99 = Get-Percentile $elapsedValues 99
$min = $elapsedValues[0]
$max = $elapsedValues[-1]
$avg = [Math]::Round(($elapsedValues | Measure-Object -Average).Average, 2)
$throughput = [Math]::Round(($totalRequests / $stopwatch.Elapsed.TotalSeconds), 2)
$errorRate = [Math]::Round((($failures / $totalRequests) * 100), 2)

Write-Host ""
Write-Host "=================== JMETER RESULTS SUMMARY ===================" -ForegroundColor Green
Write-Host (" Total Requests:      {0}" -f $totalRequests)
Write-Host (" Failed Requests:     {0} ({1}%)" -f $failures, $errorRate)
Write-Host (" Test Duration:       {0}s" -f $stopwatch.Elapsed.TotalSeconds.ToString('F2'))
Write-Host (" Throughput:          {0} req/sec" -f $throughput)
Write-Host (" Average Latency:     {0} ms" -f $avg)
Write-Host (" Min Latency:         {0} ms" -f $min)
Write-Host (" Max Latency:         {0} ms" -f $max)
Write-Host (" p50 Median Latency:  {0} ms" -f $p50)
Write-Host (" p90 Latency:         {0} ms" -f $p90)
Write-Host (" p95 Latency:         {0} ms" -f $p95)
Write-Host (" p99 Latency:         {0} ms" -f $p99)
Write-Host "==============================================================" -ForegroundColor Green

# Breakdown by Sampler
Write-Host "`nBreakdown by Endpoint/Sampler:" -ForegroundColor Cyan
$csv | Group-Object label | ForEach-Object {
    $group = $_.Group
    $latencies = $group | ForEach-Object { [int]$_.elapsed } | Sort-Object
    $count = $group.Count
    $gFail = ($group | Where-Object { $_.success -ne "true" }).Count
    $gP50 = Get-Percentile $latencies 50
    $gP95 = Get-Percentile $latencies 95
    $gP99 = Get-Percentile $latencies 99
    $gAvg = [Math]::Round(($latencies | Measure-Object -Average).Average, 2)
    Write-Host (" - {0,-35} : Count={1,-4} Failed={2,-2} Avg={3,6}ms p50={4,4}ms p95={5,4}ms p99={6,4}ms" -f `
        $_.Name, $count, $gFail, $gAvg, $gP50, $gP95, $gP99)
}

Write-Host "`nHTML Dashboard Report generated at:" -ForegroundColor Yellow
Write-Host " file://$HtmlReportDir/index.html"
Write-Host ""
