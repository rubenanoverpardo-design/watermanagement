<#
=============================================================================
 pc_operarios.ps1 · ORDENADOR DE LOS OPERARIOS (WM_FO)

 CENTRAL y Kafka ya están en Railway. El FO solo habla con Kafka, así que
 solo hace falta su dirección. Este script, en este ordenador:
   1. comprueba Docker y que Kafka responde;
   2. construye la imagen del FO;
   3. abre UNA VENTANA para cada operario.

 USO (en PowerShell, desde la raíz del repo)
   powershell -ExecutionPolicy Bypass -File scripts\pc_operarios.ps1
   powershell -ExecutionPolicy Bypass -File scripts\pc_operarios.ps1 -Parar

 Cuántos operarios (de 1 a 10; por defecto 2). Se llaman FO-01, FO-02...
 -Estaciones es el número de estaciones que se lanzaron con
 pc_estaciones.ps1 -Numero (WS-01, WS-02...):
   ... pc_operarios.ps1 -Numero 3 -Estaciones 5

 A cada operario se le GENERA un fichero de peticiones (carpeta
 peticiones_generadas\) que riega todas esas estaciones, una detrás de otra.
 Cada operario empieza por una estación distinta (FO-01 por WS-01, FO-02 por
 WS-02...) para que no pidan todos la misma a la vez. Si hay más operarios
 que estaciones, algunos coincidirán y CENTRAL denegará la petición del que
 llegue segundo (estación ocupada): es lo esperado.
   ... pc_operarios.ps1 -Numero 3 -Estaciones 5 -Segundos 15   riegos de 15 s
   ... pc_operarios.ps1 -Numero 3 -Estaciones 5 -Vueltas 2     dos pasadas
   ... pc_operarios.ps1 -Numero 3 -Manual                      sin fichero, solo menú

 Otra dirección, o una lista de operarios escrita a mano
 ('ID' o 'ID|fichero de peticiones del repo'):
   ... pc_operarios.ps1 -Kafka host:puerto
   ... pc_operarios.ps1 -Operarios 'FO-12','FO-07|WM_FO/peticiones.txt','FO-30'

 Ver lo que haría y generar los ficheros, sin arrancar nada (no necesita
 Docker ni red):
   ... pc_operarios.ps1 -Numero 3 -Estaciones 5 -Simular

 En la ventana de cada operario: 1 = ver estaciones, 2 = pedir riego (WS-04#30).
=============================================================================
#>
param(
    # TCP proxy de kafka (-> :9092) en Railway
    [string]$Kafka = 'thomas.proxy.rlwy.net:37949',
    # Cuántos operarios lanzar (1 a 10): FO-01, FO-02...
    [ValidateRange(1, 10)][int]$Numero = 2,
    # Cuántas estaciones hay lanzadas con pc_estaciones.ps1 -Numero (WS-01, WS-02...)
    [ValidateRange(1, 10)][int]$Estaciones = 2,
    # Segundos que dura cada riego del fichero generado
    [ValidateRange(1, 3600)][int]$Segundos = 10,
    # Cuántas veces recorre cada operario todas las estaciones
    [ValidateRange(1, 20)][int]$Vueltas = 1,
    # Sin fichero: los operarios solo tienen el menú manual
    [switch]$Manual,
    # O una lista a mano: 'ID' o 'ID|fichero del repo' (entonces lo anterior no se usa)
    [string[]]$Operarios = @(),
    # No arranca nada: enseña qué lanzaría y genera los ficheros de peticiones
    [switch]$Simular,
    # Para y borra los operarios de este ordenador
    [switch]$Parar
)

# 'Continue' y no 'Stop': en Windows PowerShell, con 'Stop', cualquier cosa que
# docker escriba por stderr se convierte en un error que corta el script.
$ErrorActionPreference = 'Continue'

$RepoUrl = 'https://github.com/ral44-ua/watermanagement-sd'

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
function Responde([string]$dir) {
    $h, $p = $dir.Split(':')
    $c = New-Object System.Net.Sockets.TcpClient
    try { return $c.ConnectAsync($h, [int]$p).Wait(5000) } catch { return $false } finally { $c.Close() }
}
function Q([string]$s) { "'" + ($s -replace "'", "''") + "'" }
function Corto([string]$id) { ($id -replace '-', '').ToLower() }      # FO-12 -> fo12
function Existe([string]$nombre) { [bool](docker ps -q -f "name=^$nombre$") }

# Abre una ventana nueva de PowerShell que ejecuta $comando y se queda abierta.
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

function DockerListo {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fallo 'Falta Docker. Instala Docker Desktop.' }
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { Fallo 'Docker no responde: abre Docker Desktop y espera a que arranque.' }
}

# ------------------------- Operarios y sus peticiones ------------------------
# Lista de operarios: Id, Repo (fichero que ya está en el repo) o Pc (fichero
# generado en este ordenador), y K (número de orden del operario generado).
$Operarios = @($Operarios | ForEach-Object { $_ -split ',' } | Where-Object { $_ })   # con -File, 'a','b' llega como un solo texto
$Lista = @()
if ($Operarios.Count) {
    foreach ($item in $Operarios) {
        $id, $f = $item.Split('|', 2)
        $Lista += [pscustomobject]@{ Id = $id; Repo = $f; Pc = $null; K = 0 }
    }
} else {
    foreach ($k in 1..$Numero) {
        $Lista += [pscustomobject]@{ Id = ('FO-{0:d2}' -f $k); Repo = $null; Pc = $null; K = $k }
    }
}

# Peticiones del operario número $k: todas las estaciones (WS-01..WS-N),
# empezando por la suya y dando la vuelta. Con 3 estaciones:
#   FO-01: WS-01, WS-02, WS-03      FO-02: WS-02, WS-03, WS-01
function Peticiones([int]$k) {
    $lineas = @()
    foreach ($v in 1..$Vueltas) {
        foreach ($i in 0..($Estaciones - 1)) {
            $n = (($k - 1 + $i) % $Estaciones) + 1
            $lineas += ('WS-{0:d2}#{1}' -f $n, $Segundos)
        }
    }
    return $lineas
}

# Escribe el fichero de cada operario generado en <raíz>\peticiones_generadas.
# UTF-8 sin BOM y con una petición por línea: ESTACIÓN#SEGUNDOS.
function GenerarFicheros([string]$raiz) {
    if ($Manual) { return }
    $carpeta = Join-Path $raiz 'peticiones_generadas'
    New-Item -ItemType Directory -Force -Path $carpeta | Out-Null
    foreach ($o in $Lista) {
        if (-not $o.K) { continue }                    # operario de la lista a mano
        $ruta = Join-Path $carpeta "peticiones_$($o.Id).txt"
        $cabecera = "# Peticiones de $($o.Id): $Estaciones estaciones, $Segundos s cada riego, $Vueltas vuelta(s)"
        $texto = (@($cabecera) + (Peticiones $o.K)) -join "`n"
        [IO.File]::WriteAllText($ruta, $texto + "`n", (New-Object Text.UTF8Encoding $false))
        $o.Pc = $ruta
    }
}

# --------------------------------- Simular -----------------------------------
if ($Simular) {
    GenerarFicheros (Split-Path -Parent $PSScriptRoot)
    Info "SIMULACIÓN: se lanzarían $($Lista.Count) operarios (no se arranca nada)"
    foreach ($o in $Lista) {
        if ($o.Pc) { Write-Host "   $($o.Id)  pide: $((Peticiones $o.K) -join '  ')" }
        elseif ($o.Repo) { Write-Host "   $($o.Id)  con el fichero $($o.Repo)" }
        else { Write-Host "   $($o.Id)  solo menú manual" }
    }
    if (-not $Manual -and -not $Operarios.Count) {
        Write-Host "   Ficheros generados en $(Join-Path (Split-Path -Parent $PSScriptRoot) 'peticiones_generadas')"
    }
    Write-Host "   Kafka: $Kafka"
    exit 0
}

# ---------------------------------- Parar ------------------------------------
if ($Parar) {
    DockerListo
    $n = @(docker ps -a --format '{{.Names}}' | Where-Object { $_ -match '^fo\d+$' })
    if ($n.Count) { docker rm -f @n | Out-Null; Ok "Parado: $($n -join ', ')" }
    else { Ok 'No había operarios en marcha' }
    exit 0
}

# ------------------------------ Comprobaciones -------------------------------
if ($Kafka -notmatch '^[A-Za-z0-9._-]+:\d+$') { Fallo "'$Kafka' no es una dirección host:puerto válida." }
Info 'Comprobando requisitos'
DockerListo; Ok 'Docker en marcha'
if (Responde $Kafka) { Ok "Kafka responde en $Kafka" }
else { Fallo "Kafka no responde en $Kafka. ¿Está desplegado en Railway? ¿Es el TCP proxy del puerto 9092?" }

# --------------------------------- Código ------------------------------------
$Root = Split-Path -Parent $PSScriptRoot
if (-not ((Test-Path (Join-Path $Root 'WM_FO')) -and (Test-Path (Join-Path $Root 'common')))) {
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

# --------------------------------- Imagen ------------------------------------
Info 'Construyendo la imagen (la primera vez tarda un poco)'
docker build -q -f (Join-Path $Root 'WM_FO\Dockerfile') -t wm-fo $Root | Out-Null
if ($LASTEXITCODE -ne 0) { Fallo 'No se pudo construir la imagen del FO.' }
Ok 'Imagen wm-fo'

# -------------------------------- Operarios ----------------------------------
GenerarFicheros $Root
foreach ($o in $Lista) {
    $id = $o.Id
    if ($id -notmatch '^FO-\d+$') { Fallo "ID de operario no válido: '$id' (formato FO-12)." }
    $c = Corto $id
    docker rm -f $c *> $null                 # por si quedaba uno de antes

    if ($o.Pc) {
        # Fichero generado en este ordenador: se monta dentro del contenedor
        # (solo lectura) y el FO lo lee como /app/peticiones.txt.
        $cmd = "docker run -it --rm --name $c -v $(Q ($o.Pc + ':/app/peticiones.txt:ro')) " +
               "wm-fo $(Q $Kafka) $id --fichero peticiones.txt"
        Info "$id (pide: $((Peticiones $o.K) -join ' '))"
    } elseif ($o.Repo) {
        # El fichero va dentro de la imagen (se copia WM_FO\ al construirla).
        if (-not (Test-Path (Join-Path $Root $o.Repo))) { Fallo "No existe el fichero de peticiones $($o.Repo)" }
        $cmd = "docker run -it --rm --name $c wm-fo $(Q $Kafka) $id --fichero $(Q $o.Repo)"
        Info "$id (peticiones de $($o.Repo))"
    } else {
        $cmd = "docker run -it --rm --name $c wm-fo $(Q $Kafka) $id"
        Info $id
    }
    Abrir "$id · Operario" $cmd

    # Comprobar que ha arrancado y sigue vivo
    $fin = (Get-Date).AddSeconds(30)
    while (-not (Existe $c) -and (Get-Date) -lt $fin) { Start-Sleep -Seconds 1 }
    Start-Sleep -Seconds 3
    if (Existe $c) { Ok "$id en marcha" } else { Aviso "$id no está en marcha: mira su ventana." }
}

Write-Host ''
Ok 'Operarios en marcha.'
Write-Host '   En la ventana de cada operario: 1 = ver estaciones, 2 = pedir riego (WS-04#30).'
Write-Host '   Para pararlo todo:  powershell -ExecutionPolicy Bypass -File scripts\pc_operarios.ps1 -Parar'
