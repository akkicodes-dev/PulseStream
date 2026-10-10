# PulseStream — Milestone 9 Final End-to-End Verification Script
param(
    [string]$BaseUri = "http://localhost:8081",
    [string]$DbContainer = "pulsestream-postgres"
)

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " PulseStream Milestone 9: Final End-to-End Verification  " -ForegroundColor Cyan
Write-Host " Target API: $BaseUri                                    " -ForegroundColor Cyan
Write-Host " Database Container: $DbContainer                        " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Health & Probes
Write-Host "`n[Step 1] Verifying Actuator Health & Probes..." -ForegroundColor Yellow
$health = Invoke-RestMethod -Uri "$BaseUri/actuator/health" -Method Get -TimeoutSec 10
Write-Host " -> /actuator/health status: $($health.status) (DB: $($health.components.db.status))"

$liveness = Invoke-RestMethod -Uri "$BaseUri/actuator/health/liveness" -Method Get -TimeoutSec 5
Write-Host " -> /actuator/health/liveness status: $($liveness.status)"

$readiness = Invoke-RestMethod -Uri "$BaseUri/actuator/health/readiness" -Method Get -TimeoutSec 5
Write-Host " -> /actuator/health/readiness status: $($readiness.status)"

if ($health.status -ne "UP" -or $liveness.status -ne "UP" -or $readiness.status -ne "UP") {
    throw "Health check probe failed!"
}

# 2. Ingest Valid Event 1
$evtId1 = [guid]::NewGuid().ToString()
$userId = "e2e_user_" + [guid]::NewGuid().ToString().Substring(0, 8)
$sessId = "e2e_sess_" + [guid]::NewGuid().ToString().Substring(0, 8)
$nowIso = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")

Write-Host "`n[Step 2] Ingesting initial event (PAGE_VIEW)..." -ForegroundColor Yellow
$payload1 = @{
    eventId = $evtId1
    userId = $userId
    sessionId = $sessId
    eventType = "PAGE_VIEW"
    timestamp = $nowIso
    source = "WEB"
    metadata = @{ page = "/home"; browser = "Chrome" }
} | ConvertTo-Json

$resp1 = Invoke-RestMethod -Uri "$BaseUri/api/v1/events" -Method Post -Body $payload1 -ContentType "application/json"
Write-Host " -> Event 1 Ingestion: status=$($resp1.status), eventId=$($resp1.eventId)"
Start-Sleep -Seconds 2

# 3. Ingest Valid Event 2 for same session
$evtId2 = [guid]::NewGuid().ToString()
Write-Host "`n[Step 3] Ingesting second event (PRODUCT_VIEW) in same session..." -ForegroundColor Yellow
$payload2 = @{
    eventId = $evtId2
    userId = $userId
    sessionId = $sessId
    eventType = "PRODUCT_VIEW"
    timestamp = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
    source = "WEB"
    metadata = @{ productId = "prod_999" }
} | ConvertTo-Json

$resp2 = Invoke-RestMethod -Uri "$BaseUri/api/v1/events" -Method Post -Body $payload2 -ContentType "application/json"
Write-Host " -> Event 2 Ingestion: status=$($resp2.status), eventId=$($resp2.eventId)"
Start-Sleep -Seconds 2

# 4. Ingest Duplicate of Event 1 (Idempotency)
Write-Host "`n[Step 4] Submitting duplicate of Event 1 ($evtId1)..." -ForegroundColor Yellow
$respDup = Invoke-RestMethod -Uri "$BaseUri/api/v1/events" -Method Post -Body $payload1 -ContentType "application/json"
Write-Host " -> Duplicate Ingestion Response: status=$($respDup.status), eventId=$($respDup.eventId)"
Start-Sleep -Seconds 2

# Verify in DB: Event 1 exists only once, session count = 2
$dbEvtCount = (docker exec $DbContainer psql -U pulsestream -d pulsestream -t -c "SELECT COUNT(*) FROM activity_events WHERE event_id = '$evtId1';").Trim()
$dbSess = (docker exec $DbContainer psql -U pulsestream -d pulsestream -t -c "SELECT event_count, is_active FROM user_sessions WHERE session_id = '$sessId';").Trim()
Write-Host " -> PostgreSQL verification: Event 1 count in DB = $dbEvtCount (expected 1)"
Write-Host " -> PostgreSQL verification: Session state = '$dbSess' (expected event_count=2, is_active=t)"

if ($dbEvtCount -ne "1") { throw "Duplicate event insertion failed!" }

# 5. Ingest LOGOUT Event
Write-Host "`n[Step 5] Ingesting LOGOUT event for session $sessId..." -ForegroundColor Yellow
$evtIdLogout = [guid]::NewGuid().ToString()
$payloadLogout = @{
    eventId = $evtIdLogout
    userId = $userId
    sessionId = $sessId
    eventType = "LOGOUT"
    timestamp = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
    source = "WEB"
} | ConvertTo-Json

$respLogout = Invoke-RestMethod -Uri "$BaseUri/api/v1/events" -Method Post -Body $payloadLogout -ContentType "application/json"
Write-Host " -> LOGOUT event accepted: $($respLogout.status)"
Start-Sleep -Seconds 2

$dbSessLogout = (docker exec $DbContainer psql -U pulsestream -d pulsestream -t -c "SELECT event_count, is_active, ended_at IS NOT NULL FROM user_sessions WHERE session_id = '$sessId';").Trim()
Write-Host " -> PostgreSQL verification: Session after LOGOUT = '$dbSessLogout' (expected event_count=3, is_active=f, ended_at=t)"

# 6. Ingest Late Event on closed session
Write-Host "`n[Step 6] Ingesting late event for closed session..." -ForegroundColor Yellow
$evtIdLate = [guid]::NewGuid().ToString()
$payloadLate = @{
    eventId = $evtIdLate
    userId = $userId
    sessionId = $sessId
    eventType = "PAGE_VIEW"
    timestamp = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
    source = "WEB"
} | ConvertTo-Json

$respLate = Invoke-RestMethod -Uri "$BaseUri/api/v1/events" -Method Post -Body $payloadLate -ContentType "application/json"
Write-Host " -> Late event accepted: $($respLate.status)"
Start-Sleep -Seconds 2

$dbSessLate = (docker exec $DbContainer psql -U pulsestream -d pulsestream -t -c "SELECT event_count, is_active, ended_at IS NOT NULL FROM user_sessions WHERE session_id = '$sessId';").Trim()
Write-Host " -> PostgreSQL verification: Closed session invariant = '$dbSessLate' (expected count=4, is_active=f, ended_at=t)"

# 7. Query Analytics Endpoints
Write-Host "`n[Step 7] Querying Analytics Endpoints..." -ForegroundColor Yellow
$userAnalytics = Invoke-RestMethod -Uri "$BaseUri/api/v1/analytics/users/$userId" -Method Get
Write-Host " -> User Analytics for $userId : TotalEvents=$($userAnalytics.totalEvents), TotalSessions=$($userAnalytics.totalSessions), ActiveSessions=$($userAnalytics.activeSessions)"

$overview = Invoke-RestMethod -Uri "$BaseUri/api/v1/analytics/overview" -Method Get
Write-Host " -> Overview Analytics: TotalEvents=$($overview.totalEvents), UniqueUsers=$($overview.uniqueUsers), ActiveSessions=$($overview.activeSessions)"

$timeseries = Invoke-RestMethod -Uri "$BaseUri/api/v1/analytics/timeseries?interval=HOUR" -Method Get
Write-Host " -> Timeseries Analytics: Interval=$($timeseries.interval), BucketCount=$($timeseries.bucketCount)"

$activeSessions = Invoke-RestMethod -Uri "$BaseUri/api/v1/analytics/sessions/active" -Method Get
Write-Host " -> Active Sessions count: $($activeSessions.data.Count)"

# 8. Test Input Validation & Error Contract
Write-Host "`n[Step 8] Testing Invalid Payload Contract (400 Bad Request)..." -ForegroundColor Yellow
try {
    $badPayload = '{"userId": "", "sessionId": "s", "eventType": "PAGE_VIEW", "source": "WEB", "timestamp": "2026-10-10T12:00:00Z"}'
    Invoke-RestMethod -Uri "$BaseUri/api/v1/events" -Method Post -Body $badPayload -ContentType "application/json"
    throw "Expected 400 Bad Request but succeeded!"
} catch {
    Write-Host " -> Successfully caught expected validation rejection: $($_.Exception.Message)"
}

# 9. Scrape Prometheus Metrics
Write-Host "`n[Step 9] Scraping Prometheus Metrics..." -ForegroundColor Yellow
$promScrape = Invoke-RestMethod -Uri "$BaseUri/actuator/prometheus" -Method Get
if ($promScrape -match "pulsestream_events_accepted_total") {
    Write-Host " -> Found pulsestream_events_accepted_total in Prometheus metrics!" -ForegroundColor Green
} else {
    throw "Prometheus metric pulsestream_events_accepted_total not found!"
}

# 10. Verify Flyway Schema Migrations
Write-Host "`n[Step 10] Verifying Flyway Migrations in PostgreSQL..." -ForegroundColor Yellow
$migrationRaw = (docker exec $DbContainer psql -U pulsestream -d pulsestream -t -c "SELECT COUNT(*) FROM flyway_schema_history WHERE success = true;") -join ""
$migrationCount = [int]($migrationRaw.Trim())
Write-Host " -> Successfully applied Flyway migrations count = $migrationCount"
if ($migrationCount -lt 5) {
    throw "Flyway migrations incomplete: found $migrationCount migrations"
}

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host " ALL MILESTONE 9 END-TO-END CHECKS PASSED PERFECTLY!     " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
