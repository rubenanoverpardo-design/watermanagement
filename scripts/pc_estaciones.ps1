<#
=============================================================================
 pc_estaciones.ps1 · ORDENADOR DE LAS ESTACIONES (WM_WS_M + WM_WS_E)

 CENTRAL y Kafka ya están en Railway. Este script, en este ordenador:
   1. comprueba Docker y que CENTRAL y Kafka responden;
   2. construye las imágenes del Monitor y del Engine;
   3. abre UNA VENTANA para cada Monitor y otra para cada Engine.
      Primero el Monitor (se autentica en CENTRAL); cuando lo consigue,
      su Engine (se conecta a su Monitor y a Kafka).

 USO (en PowerShell, desde la raíz del repo)
   powershell -ExecutionPolicy Bypass -File scripts\pc_estaciones.ps1
   powershell -ExecutionPolicy Bypass -File scripts\pc_estaciones.ps1 -Parar

 Cuántas estaciones (de 1 a 10; por defecto 2). Se llaman WS-01, WS-02...
 y cada una tiene su parque:
   ... pc_estaciones.ps1 -Numero 5

 Otras direcciones, o una lista de estaciones escrita a mano:
   ... pc_estaciones.ps1 -Central host:puerto -Kafka host:puerto
   ... pc_estaciones.ps1 -Estaciones 'WS-04|River Park','WS-20|Plaza Mayor'

 Ver lo que haría, sin arrancar nada (no necesita Docker ni red):
   ... pc_estaciones.ps1 -Numero 5 -Simular

 En la ventana de cada Engine: k = avería on/off, p = parar riego,
 r = pedir riego, q = salir.
=============================================================================
#>
param(
    # TCP proxy de wm-central (-> :9000) en Railway
    [string]$Central = 'yamanote.proxy.rlwy.net:24727',
    # TCP proxy de kafka (-> :9092) en Railway
    [string]$Kafka = 'thomas.proxy.rlwy.net:37949',
    # Cuántas estaciones lanzar (1 a 10): WS-01, WS-02... cada una con su parque
    [ValidateRange(1, 10)][int]$Numero = 2,
    # O una lista a mano de 'ID|Ubicación' (entonces -Numero no se usa)
    [string[]]$Estaciones = @(),
    # No arranca nada: solo enseña qué estaciones lanzaría
    [switch]$Simular,
    # Para y borra las estaciones de este ordenador
    [switch]$Parar
)

# 'Continue' y no 'Stop': en Windows PowerShell, con 'Stop', cualquier cosa que
# docker escriba por stderr se convierte en un error que corta el script.
# Los fallos se comprueban a mano con $LASTEXITCODE.
$ErrorActionPreference = 'Continue'

$RepoUrl      = 'https://github.com/ral44-ua/watermanagement-sd'
$Red          = 'wm'      # red de Docker: el Engine encuentra a su Monitor por su nombre
$PuertoEngine = 9100      # puerto Monitor<->Engine dentro de esa red (no se publica)

# Ubicación de las estaciones que se generan con -Numero (WS-01 es la primera).
$Parques = @('River Park', 'Riverside Gardens', 'Central Park', 'Botanic Garden',
             'Castle Gardens', 'Station Square', 'Harbour Park', 'University Campus',
             'Old Town Square', 'Lakeside Park')

# Sin lista a mano, se generan las estaciones: WS-01|River Park, WS-02|...
$Estaciones = @($Estaciones | ForEach-Object { $_ -split ',' } | Where-Object { $_ })   # con -File, 'a','b' llega como un solo texto
$Generadas = -not $Estaciones.Count
if ($Generadas) {
    $Estaciones = 1..$Numero | ForEach-Object { 'WS-{0:d2}|{1}' -f $_, $Parques[$_ - 1] }
}

# --------------------------------- Mensajes ----------------------------------
function Info([string]$t)  { Write-Host "==> $t" -ForegroundColor Cyan }
function Ok([string]$t)    { Write-Host " OK $t" -ForegroundColor Green }
function Aviso([string]$t) { Write-Host "AVISO $t" -ForegroundColor Yellow }
function Fallo([string]$t) {
    Write-Host "ERROR $t" -ForegroundColor Red
    Read-Host 'Pulsa Enter para salir' | Out-Null
    exit 1
}

# --------------------------------- Utilidades --------------------------------
function Valida([string]$dir, [string]$que) {
    if ($dir -notmatch '^[A-Za-z0-9._-]+:\d+$') { Fallo "'$dir' no es una dirección host:puerto válida ($que)." }
}

# ¿Hay algo escuchando en host:puerto? (espera como mucho 5 s)
function Responde([string]$dir) {
    $h, $p = $dir.Split(':')
    $c = New-Object System.Net.Sockets.TcpClient
    try { return $c.ConnectAsync($h, [int]$p).Wait(5000) } catch { return $false } finally { $c.Close() }
}

# Texto entre comillas simples de PowerShell (una ' dentro se escribe '').
function Q([string]$s) { "'" + ($s -replace "'", "''") + "'" }

# WS-04 -> ws04 (nombre de contenedor y de volumen)
function Corto([string]$id) { ($id -replace '-', '').ToLower() }

function Existe([string]$nombre) { [bool](docker ps -q -f "name=^$nombre$") }

# Abre una ventana nueva de PowerShell que ejecuta $comando y se queda abierta.
# Se pasa con -EncodedCommand (Base64) para no pelearse con las comillas.
$PsExe = (Get-Process -Id $PID).Path
function Abrir([string]$titulo, [string]$comando) {
    $s = @"
`$Host.UI.RawUI.WindowTitle = $(Q $titulo)
Write-Host '$titulo' -ForegroundColor Cyan
$comando
Write-Host ''
Write-Host 'El programa ha terminado. Puedes cerrar esta ventana.' -ForegroundColor Yellow
"@
    $enc = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($s))
    Start-Process -FilePath $PsExe -ArgumentList '-NoExit', '-NoProfile', '-EncodedCommand', $enc | Out-Null
}

# Espera a que el contenedor escriba $texto en su salida.
function EsperarLog([string]$nombre, [string]$texto, [int]$segundos) {
    $fin = (Get-Date).AddSeconds($segundos); $visto = $false
    while ((Get-Date) -lt $fin) {
        if (Existe $nombre) {
            $visto = $true
            if ((docker logs $nombre 2>&1 | Out-String).Contains($texto)) { return $true }
        } elseif ($visto) {
            return $false          # arrancó y se ha parado: algo ha fallado
        }
        Start-Sleep -Seconds 2
    }
    return $false
}

function DockerListo {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fallo 'Falta Docker. Instala Docker Desktop.' }
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { Fallo 'Docker no responde: abre Docker Desktop y espera a que arranque.' }
}

# ---------------------------------- Parar ------------------------------------
if ($Parar) {
    DockerListo
    $n = @(docker ps -a --format '{{.Names}}' | Where-Object { $_ -match '^ws\d+-[me]$' })
    if ($n.Count) { docker rm -f @n | Out-Null; Ok "Parado: $($n -join ', ')" }
    else { Ok 'No había estaciones en marcha' }
    exit 0
}

# --------------------------------- Simular -----------------------------------
if ($Simular) {
    Info "SIMULACIÓN: se lanzarían $($Estaciones.Count) estaciones (no se arranca nada)"
    foreach ($item in $Estaciones) {
        $id, $ubic = $item.Split('|', 2)
        Write-Host ('   {0}  {1,-20} contenedores {2}-m (Monitor) y {2}-e (Engine)' -f $id, $ubic, (Corto $id))
    }
    Write-Host "   CENTRAL: $Central    Kafka: $Kafka"
    exit 0
}

# ------------------------------ Comprobaciones -------------------------------
Valida $Central 'CENTRAL'; Valida $Kafka 'Kafka'
Info 'Comprobando requisitos'
DockerListo; Ok 'Docker en marcha'
if (Responde $Central) { Ok "CENTRAL responde en $Central" }
else { Fallo "CENTRAL no responde en $Central. ¿Está desplegada en Railway? ¿Es el TCP proxy del puerto 9000?" }
if (Responde $Kafka) { Ok "Kafka responde en $Kafka" }
else { Fallo "Kafka no responde en $Kafka. ¿Está desplegado en Railway? ¿Es el TCP proxy del puerto 9092?" }

# --------------------------------- Código ------------------------------------
# Si el script está en scripts\ del repo, usa ese repo. Si no, lo descarga.
$Root = Split-Path -Parent $PSScriptRoot
if (-not ((Test-Path (Join-Path $Root 'WM_WS')) -and (Test-Path (Join-Path $Root 'common')))) {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) { Fallo 'Falta git para descargar el repositorio.' }
    $Root = Join-Path $HOME 'watermanagement-sd'
    if (-not (Test-Path (Join-Path $Root '.git'))) {
        Info "Descargando el repositorio en $Root"
        git clone --depth 1 $RepoUrl $Root
        if ($LASTEXITCODE -ne 0) { Fallo 'No se pudo descargar el repositorio.' }
    }
}
if (Get-Command git -ErrorAction SilentlyContinue) {
    git -C $Root pull --ff-only *> $null
    if ($LASTEXITCODE -eq 0) { Ok 'Código al día' } else { Aviso 'Sin git pull: se usa el código que hay' }
}

# ---------------------------- Imágenes y red ---------------------------------
Info 'Construyendo imágenes (la primera vez tarda un poco)'
docker build -q -f (Join-Path $Root 'WM_WS\WS_M\Dockerfile') -t wm-ws-m $Root | Out-Null
if ($LASTEXITCODE -ne 0) { Fallo 'No se pudo construir la imagen del Monitor.' }
Ok 'Imagen wm-ws-m'
docker build -q -f (Join-Path $Root 'WM_WS\WS_E\Dockerfile') -t wm-ws-e $Root | Out-Null
if ($LASTEXITCODE -ne 0) { Fallo 'No se pudo construir la imagen del Engine.' }
Ok 'Imagen wm-ws-e'
docker network inspect $Red *> $null
if ($LASTEXITCODE -ne 0) { docker network create $Red | Out-Null }

# ------------------------------- Estaciones ----------------------------------
foreach ($item in $Estaciones) {
    $id, $ubic = $item.Split('|', 2)
    if (-not $ubic) { $ubic = 'Sin ubicación' }
    if ($id -notmatch '^WS-\d+$') { Fallo "ID de estación no válido: '$id' (formato WS-04)." }
    $c = Corto $id
    docker rm -f "$c-m" "$c-e" *> $null       # por si quedaba una de antes

    Info "$id ($ubic)"
    # 1) Monitor, en su ventana
    Abrir "$id · Monitor" ("docker run -it --rm --name $c-m --network $Red wm-ws-m " +
        "$PuertoEngine $(Q $Central) $id --ubicacion $(Q $ubic)")
    if (EsperarLog "$c-m" 'Autenticado en CENTRAL' 60) { Ok 'Monitor autenticado en CENTRAL' }
    else { Aviso "El Monitor de $id no se ha autenticado en 60 s: mira su ventana." }

    # 2) Engine, en su ventana. El volumen guarda el riego en curso por si se cae.
    Abrir "$id · Engine" ("docker run -it --rm --name $c-e --network $Red -v wm-${c}:/datos wm-ws-e " +
        "$(Q $Kafka) ${c}-m:$PuertoEngine")
    if (EsperarLog "$c-e" 'Conectado al Monitor' 60) { Ok 'Engine conectado a su Monitor' }
    else { Aviso "El Engine de $id no se ha conectado en 60 s: mira su ventana." }
}

Write-Host ''
Ok 'Estaciones en marcha: deberían salir en verde en el panel de CENTRAL.'
Write-Host '   En la ventana de cada Engine: k = avería, p = parar riego, r = pedir riego, q = salir.'
Write-Host '   Para pararlo todo:  powershell -ExecutionPolicy Bypass -File scripts\pc_estaciones.ps1 -Parar'
if ($Generadas) {
    Write-Host ''
    Write-Host '   En el ordenador de los operarios, para que pidan riegos a ESTAS estaciones'
    Write-Host '   (cambia el 2 por el número de operarios que quieras, de 1 a 10):'
    Write-Host "     powershell -ExecutionPolicy Bypass -File scripts\pc_operarios.ps1 -Numero 2 -Estaciones $($Estaciones.Count)" -ForegroundColor Cyan
}
