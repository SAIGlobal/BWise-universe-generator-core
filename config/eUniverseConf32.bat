echo Starting %*
@echo off



FOR /F "skip=2 tokens=2,*" %%A IN ('REG.exe query "HKEY_LOCAL_MACHINE\SOFTWARE\SAP BusinessObjects\Suite XI 4.0\Shared" /v "CommonFiles"') DO set "InstallPath=%%B"
set SAPBOInstallPath=%InstallPath%win32_x86
echo %SAPBOInstallPath%

set CUSTOMJAVAPATH=%SAPBOInstallPath%\jre8
Rem IF "%JAVAPATH%"=="" set JAVA_HOME=%CUSTOMJAVAPATH%

set JAVA_HOME=%CUSTOMJAVAPATH%




SET PATH=%JAVA_HOME%\bin;%SAPBOInstallPath%;%PATH%

"%JAVA_HOME%\bin\java.exe"  -version

"%JAVA_HOME%\bin\java.exe"   -Dbusinessobjects.connectivity.directory="%InstallPath%\dataAccess\connectionServer" -cp eUniverse.jar;"%InstallPath%\\java\\lib/*" com.bwise.eUniverse.Main -conf %1 
