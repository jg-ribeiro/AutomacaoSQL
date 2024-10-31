# Obtém o nome da branch via input
$branchName = Read-Host "Digite o nome da branch atual"

# Obtém a data atual no formato desejado
$dateStamp = Get-Date -Format "yyyyMMdd-HHmm"

# Cria o nome da pasta com o formato especificado
$folderName = "build\$branchName-$dateStamp"

Write-Output $folderName

# Cria a pasta de destino
New-Item -ItemType Directory -Path $folderName

# Lista de arquivos que você deseja copiar
$filesToCopy = @("main.py", "auxiliares.py", "oracle.py", "access.py")

# Copia os arquivos para a pasta de destino
foreach ($file in $filesToCopy) {
    Copy-Item -Path $file -Destination $folderName
}

# Executa o comando pyinstaller na pasta de destino
Set-Location $folderName
Invoke-Expression "pyinstaller --onefile main.py"
