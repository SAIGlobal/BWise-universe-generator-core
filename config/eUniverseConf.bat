echo Starting %*
@echo off
FOR /F "skip=2 tokens=2,*" %%A IN ('REG.exe query "HKEY_LOCAL_MACHINE\SOFTWARE\SAP BusinessObjects\Suite XI 4.0\Shared" /v "CommonFiles"') DO set "InstallPath=%%B"
set SAPBOInstallPath=%InstallPath%win32_x86
echo %SAPBOInstallPath%

SET PATH=%SAPBOInstallPath%;%PATH%

java -d32 -Dbusinessobjects.connectivity.directory="%InstallPath%\dataAccess\connectionServer" -jar eUniverse.jar -conf eUniverse.ini
