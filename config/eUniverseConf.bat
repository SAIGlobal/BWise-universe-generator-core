echo Starting %*
@echo off

set CUSTOMJAVAPATH=C:\Program Files\Java\jre1.8.0_241
IF "%JAVAPATH%"=="" set JAVA_HOME=%CUSTOMJAVAPATH%

java -version

FOR /F "skip=2 tokens=2,*" %%A IN ('REG.exe query "HKEY_LOCAL_MACHINE\SOFTWARE\SAP BusinessObjects\Suite XI 4.0\Shared" /v "CommonFiles"') DO set "InstallPath=%%B"
set SAPBOInstallPath=%InstallPath%win64_x64
echo %SAPBOInstallPath%


SET PATH=%JAVA_HOME%\bin;%SAPBOInstallPath%;%PATH%

"%JAVA_HOME%\bin\java.exe"   -Dbusinessobjects.connectivity.directory="%InstallPath%\dataAccess\connectionServer" -cp eUniverse.jar;"%InstallPath%\\java\\lib/*" com.bwise.eUniverse.Main -conf %1 
